import math
import singer
import json
import os

from .util import Util
from datetime import datetime, timedelta
import dateutil.tz as dtz
from dateutil.parser import parse
from tap_framework.streams import BaseStream
from tap_framework.config import get_config_start_date
from tap_chargebee.state import get_last_record_value_for_table, incorporate, \
    save_state

LOGGER = singer.get_logger()


class CbTransformer(singer.Transformer):

    def log_warning(self):
        if self.filtered:
            LOGGER.debug("Filtered %s paths during transforms "
                        "as they were unsupported or not selected:\n\t%s",
                        len(self.filtered),
                        "\n\t".join(sorted(self.filtered)))
            # Output list format to parse for reporting
            LOGGER.debug("Filtered paths list: %s",
                        sorted(self.filtered))

        if self.removed:
            LOGGER.debug("Removed %s paths during transforms:\n\t%s",
                           len(self.removed),
                           "\n\t".join(sorted(self.removed)))
            # Output list format to parse for reporting
            LOGGER.debug("Removed paths list: %s", sorted(self.removed))


class BaseChargebeeStream(BaseStream):
    ENTITY = None
    END_TIMESTAMP = int(datetime.utcnow().timestamp())
    TIMEZONE = dtz.gettz('UTC')

    def __init__(self, config, state, catalog, client):
        super().__init__(config, state, catalog, client)

        if not self.config.get("end_date", False):
            # Only do this if it's a scheduled job and use_end_date is set as True
            use_end_date = config.get("use_end_date", True) # true as default
            if config.get("timezone") and os.environ.get("SCHEDULED_JOB") and use_end_date:
                # Calculate yesterday based on the timezone set in the tap-chargebee config
                timezone = config["timezone"]
                tz = dtz.gettz(timezone)
                self.TIMEZONE = tz
                yesterday = datetime.now(tz) - timedelta(days=1)
                # set the endDate to 11:59:59 yesterday
                end_date = yesterday.replace(hour=23, minute=59, second=59)
                self.END_TIMESTAMP = int(end_date.timestamp())
        else:
            raw_end_date = self.config.get("end_date")
            end_date = parse(raw_end_date)
            # Date-only strings have no time component; normalize to end-of-day so
            # the full calendar day is included. Explicit times are left as-is.
            if "T" not in raw_end_date and ":" not in raw_end_date:
                end_date = end_date.replace(hour=23, minute=59, second=59)
            if self.config.get("start_date"):
                start_date = parse(self.config.get("start_date"))
                if end_date.timestamp() <= start_date.timestamp():
                    raise ValueError(
                        f"end_date ({end_date.date()}) must be after start_date ({start_date.date()})"
                    )
            self.END_TIMESTAMP = int(end_date.timestamp())

    def write_schema(self):
        singer.write_schema(
            self.catalog.stream,
            self.catalog.schema.to_dict(),
            key_properties=self.KEY_PROPERTIES,
            bookmark_properties=self.BOOKMARK_PROPERTIES)

    def generate_catalog(self):
        schema = self.get_schema()
        mdata = singer.metadata.new()

        metadata = {

            "forced-replication-method": self.REPLICATION_METHOD,
            "valid-replication-keys": self.VALID_REPLICATION_KEYS,
            "inclusion": self.INCLUSION,
            #"selected-by-default": self.SELECTED_BY_DEFAULT,
            "table-key-properties": self.KEY_PROPERTIES
        }

        for k, v in metadata.items():
            mdata = singer.metadata.write(
                mdata,
                (),
                k,
                v
            )

        for field_name, field_schema in schema.get('properties').items():
            inclusion = 'available'

            if field_name in self.KEY_PROPERTIES or field_name in self.BOOKMARK_PROPERTIES:
                inclusion = 'automatic'

            mdata = singer.metadata.write(
                mdata,
                ('properties', field_name),
                'inclusion',
                inclusion
            )

        cards = singer.utils.load_json(
            os.path.normpath(
                os.path.join(
                    self.get_class_path(),
                    '../schemas/{}.json'.format("cards"))))

        refs = {"cards.json": cards}

        return [{
            'tap_stream_id': self.TABLE,
            'stream': self.TABLE,
            'schema': singer.resolve_schema_references(schema, refs),
            'metadata': singer.metadata.to_list(mdata)
        }]

    def appendCustomFields(self, record):
        listOfCustomFieldObj = ['addon', 'plan', 'subscription', 'customer', 'item']
        custom_fields = {}
        event_custom_fields = {}
        if self.ENTITY == 'event':
            content = record['content']
            words = record['event_type'].split("_")
            sl = slice(len(words) - 1)
            content_obj = "_".join(words[sl])

            if content_obj in listOfCustomFieldObj:
                for k in record['content'][content_obj].keys():
                    if "cf_" in k:
                        event_custom_fields[k] = record['content'][content_obj][k]
                record['content'][content_obj]['custom_fields'] = json.dumps(event_custom_fields)

        if record:
            for key in record.keys():
                if "cf_" in key:
                    custom_fields[key] = record[key]
            if custom_fields:
                record['custom_fields'] = json.dumps(custom_fields)
        return record

    # This overrides the transform_record method in the Fistown Analytics tap-framework package
    def transform_record(self, record):
        with CbTransformer(integer_datetime_fmt="unix-seconds-integer-datetime-parsing") as tx:
            metadata = {}

            record = self.appendCustomFields(record)

            if self.catalog.metadata is not None:
                metadata = singer.metadata.to_map(self.catalog.metadata)

            return tx.transform(
                record,
                self.catalog.schema.to_dict(),
                metadata)

    def get_stream_data(self, data):
        entity = self.ENTITY
        return [self.transform_record(item.get(entity)) for item in data]

    def get_deduplication_key(self, record):
        record_date = record.get('generated_at', record.get('created_at', record.get('date', '')))
        return f"{record['id']}_{record_date}_{record.get('deleted', False)}"

    def sync_data(self):
        table = self.TABLE
        api_method = self.API_METHOD
        
        # Attempt to get the bookmark date from the state file
        LOGGER.info('Attempting to get the most recent bookmark_date for entity {}.'.format(self.ENTITY))
        bookmark_date = get_last_record_value_for_table(self.state, table, 'bookmark_date')

        # If there is no bookmark date, fall back to using the start date from the config
        if bookmark_date is None:
            LOGGER.info('Could not locate bookmark_date from STATE file. Falling back to start_date from config.json instead.')
            bookmark_date = get_config_start_date(self.config)
        else:
            bookmark_date = parse(bookmark_date)

        # Convert bookmarked start date to datetime
        current_window_start_dt = datetime.fromtimestamp(int(bookmark_date.timestamp()))
        
        batching_requests = True
        batch_size_in_months = self.config.get("batch_size_in_months")
        if batch_size_in_months:
            batch_size_in_months = min(batch_size_in_months, 12)
        else:
            batching_requests = False

        previous_max_date = None

        if bookmark_date and int(bookmark_date.timestamp()) >= self.END_TIMESTAMP:
            LOGGER.warning(
                "end_date is at or before the current bookmark (%s); no records will be synced.",
                bookmark_date.isoformat(),
            )

        while math.ceil(current_window_start_dt.timestamp()) < self.END_TIMESTAMP:
            if batching_requests:
                # Calculate end of current month
                current_window_end_dt = (current_window_start_dt + timedelta(days=batch_size_in_months * 31)).replace(day=1)

                # Ensure we don't go beyond END_TIMESTAMP
                current_window_end_dt = min(current_window_end_dt,
                                        datetime.fromtimestamp(self.END_TIMESTAMP))
            else:
                current_window_end_dt = datetime.fromtimestamp(self.END_TIMESTAMP)
            
            # Convert to timestamps for the API
            current_window_start = int(current_window_start_dt.timestamp())
            current_window_end = int(current_window_end_dt.timestamp())
            
            LOGGER.info(f"Syncing {table} from {current_window_start_dt.strftime('%Y-%m-%d')} "
                       f"to {current_window_end_dt.strftime('%Y-%m-%d')}")
            
            fall_back_date_field = None
            sync_by_date = False
            # Initialize params based on entity type
            if self.ENTITY == 'event':
                params = {"occurred_at[after]": current_window_start, "occurred_at[before]": current_window_end}
                event_types = self.config.get("event_types")
                if event_types:
                    if isinstance(event_types, str):
                        event_types = event_types.split(",")
                        event_types = [event.strip() for event in event_types]
                    params.update({"event_type[in]": str(event_types)})
                bookmark_key = 'occurred_at'
            elif self.ENTITY == 'promotional_credit':
                params = {"created_at[after]": current_window_start, "created_at[before]": current_window_end}
                bookmark_key = 'created_at'
            elif self.ENTITY == 'transaction':
                params = {"updated_at[after]": current_window_start, "updated_at[before]": current_window_end}
                bookmark_key = 'updated_at'
            elif self.ENTITY in ['customer', 'invoice', 'unbilled_charge']:
                params = {"updated_at[after]": current_window_start, "updated_at[before]": current_window_end}
                bookmark_key = 'updated_at'
                if self.ENTITY in ['invoice'] and self.config.get('exclude_zero_invoices'):
                    params['total[is_not]'] = 0
            else:
                params = {"updated_at[after]": current_window_start, "updated_at[before]": current_window_end}
                bookmark_key = 'updated_at'

            if self.ENTITY in ['transaction', 'credit_note', 'invoice']:
                fall_back_date_field = 'date'
                sync_by_date = True
            
            if self.ENTITY in ['coupon', 'customer', 'subscription']:
                fall_back_date_field = 'created_at'
                sync_by_date = True

            # Apply additional filters if configured
            if self.config.get("filters"):
                entity_filters = self.config.get("filters", {}).get(self.ENTITY, {})
                for field_name, field_value in entity_filters.items():
                    params[field_name] = field_value
                    
            if self.config.get('include_deprecated') is True and self.TABLE in ['customers', 'subscriptions']:
                params["include_deprecated"] = "true"
            
            if hasattr(self, 'SORT_BY'):
                params['sort_by[asc]'] = self.SORT_BY

            # Process current window
            done = False
            ids = set()
            syncing_by_fallback_date = False  # Add flag to prevent infinite loop
            
            # records updated before `datetime_to_sync_by_date` do not have `updated_at` set,
            # so if the sync window is on or before this date we'll sync by fallback date field
            # else we only sync by updated_at
            datetime_to_sync_by_date = datetime(2016, 9, 30)
            if sync_by_date and \
                current_window_start_dt > datetime_to_sync_by_date and \
                current_window_end_dt > datetime_to_sync_by_date:
                sync_by_date = False

            while not done:
                max_date = bookmark_date
                response = self.client.make_request(
                    url=self.get_url(),
                    method=api_method,
                    params=params)

                if 'api_error_code' in response.keys():
                    if response['api_error_code'] == 'configuration_incompatible':
                        LOGGER.error('{} is not configured'.format(response['error_code']))
                        break

                records = response.get('list', [])
                to_write = self.get_stream_data(records)

                if self.ENTITY in ['transaction', 'credit_note', 'invoice',
                                    'coupon', 'customer', 'subscription']:
                    # store ids to clean duplicates
                    to_write = [record for record in to_write if self.get_deduplication_key(record) not in ids]
                    ids.update([self.get_deduplication_key(record) for record in to_write])

                if self.ENTITY == 'event':
                    for event in to_write:
                        if event["event_type"] == 'plan_deleted':
                            Util.plans.append(event['content']['plan'])
                        elif event['event_type'] == 'addon_deleted':
                            Util.addons.append(event['content']['addon'])
                        elif event['event_type'] == 'coupon_deleted':
                            Util.coupons.append(event['content']['coupon'])
                if self.ENTITY == 'plan':
                    for plan in Util.plans:
                        to_write.append(plan)
                if self.ENTITY == 'addon':
                    for addon in Util.addons:
                        to_write.append(addon)
                if self.ENTITY == 'coupon':
                    for coupon in Util.coupons:
                        to_write.append(coupon)
                if self.ENTITY == 'business_entity':
                    filtered_records = []
                    for record in to_write:
                        try:
                            # Parse ISO format datetime string to timestamp
                            updated_at = int(parse(record['updated_at']).timestamp())
                            if updated_at > current_window_start and updated_at < current_window_end:
                                filtered_records.append(record)
                        except (ValueError, TypeError) as e:
                            LOGGER.warning(f"Could not process record timestamp: {record.get('updated_at')} - {str(e)}")
                    to_write = filtered_records

                with singer.metrics.record_counter(endpoint=table) as ctr:
                    singer.write_records(table, to_write)
                    ctr.increment(amount=len(to_write))

                    # get the max date from the records
                    # if we're syncing by fallback date we don't wanna update
                    # the bookmark date because it can become inconsistent
                    if bookmark_key is not None and not syncing_by_fallback_date:
                        for item in to_write:
                            if item:
                                if item.get(bookmark_key) is not None:
                                    try:
                                        # added ignoretz to fix the issue: can't compare offset-naive and offset-aware datetimes
                                        max_date = max(
                                            max_date,
                                            parse(item.get(bookmark_key), ignoretz=True)
                                        )
                                    except TypeError:
                                        max_date = max(
                                            max_date,
                                            datetime.fromtimestamp(int(item.get(bookmark_key)), tz=dtz.gettz('UTC')
                                        ))

                # update the state with the max date after each iteration
                # only update the state if the max date has changed
                if bookmark_key is not None and not syncing_by_fallback_date and previous_max_date != max_date:
                    self.state = incorporate(
                        self.state, table, 'bookmark_date', max_date)
                    previous_max_date = max_date
                    save_state(self.state)

                if not response.get('next_offset'):
                    if sync_by_date and not syncing_by_fallback_date and fall_back_date_field:
                        params = {
                            f"{fall_back_date_field}[after]": current_window_start,
                            f"{fall_back_date_field}[before]": current_window_end,
                            "sort_by[asc]": fall_back_date_field
                        }
                        sync_by_date = False
                        syncing_by_fallback_date = True
                        LOGGER.info(f"Attempting to sync by {fall_back_date_field}.")
                    else:
                        LOGGER.info("Final offset reached. Ending sync.")
                        done = True
                else:
                    params['offset'] = response.get('next_offset')
                    LOGGER.info(f"Advancing by one offset within window [{params}]")

            # Move to next window
            current_window_start_dt = current_window_end_dt

    def sync_parent_data(self):
        table = self.TABLE
        api_method = self.API_METHOD
        done = False

        # Attempt to get the bookmark date from the state file (if one exists and is supplied).
        LOGGER.info('Attempting to get the most recent bookmark_date for entity {}.'.format(self.ENTITY))
        bookmark_date = get_last_record_value_for_table(self.state, table, 'bookmark_date')

        # If there is no bookmark date, fall back to using the start date from the config file.
        if bookmark_date is None:
            LOGGER.info('Could not locate bookmark_date from STATE file. Falling back to start_date from config.json instead.')
            bookmark_date = get_config_start_date(self.config)
        else:
            bookmark_date = parse(bookmark_date)

        # Convert bookmarked start date to POSIX.
        bookmark_date_posix = int(bookmark_date.timestamp())

        if bookmark_date_posix >= self.END_TIMESTAMP:
            LOGGER.warning(
                "end_date is at or before the current bookmark (%s); no records will be synced.",
                bookmark_date.isoformat(),
            )
            return

        sync_by_date = False
        date_sync_attempted = False  # Add flag to prevent infinite loop

        # Create params for filtering
        if self.ENTITY == 'event':
            params = {"occurred_at[after]": bookmark_date_posix, "occurred_at[before]": self.END_TIMESTAMP}
        elif self.ENTITY == 'promotional_credit':
            params = {"created_at[after]": bookmark_date_posix, "created_at[before]": self.END_TIMESTAMP}
        elif self.ENTITY == 'transaction':
            params = {"updated_at[after]": bookmark_date_posix, "updated_at[before]": self.END_TIMESTAMP}
            sync_by_date = True
        elif self.ENTITY in ['customer', 'invoice', 'unbilled_charge']:
            params = {"updated_at[after]": bookmark_date_posix, "updated_at[before]": self.END_TIMESTAMP}
            if self.ENTITY in ['invoice'] and self.config.get('exclude_zero_invoices'):
                params['total[is_not]'] = 0
        else:
            params = {"updated_at[after]": bookmark_date_posix, "updated_at[before]": self.END_TIMESTAMP}
        
        if hasattr(self, 'SORT_BY'):
            params['sort_by[asc]'] = self.SORT_BY

        LOGGER.info("Querying {} starting at {}".format(table, bookmark_date))

        ids = set()
        while not done:
            try:
                response = self.client.make_request(
                    url=self.get_url(),
                    method=api_method,
                    params=params)
            except:
                response = {}

            records = response.get('list', [])

            for record in records:
                # clean duplicate values for transactions
                if self.ENTITY == 'transaction':
                    composite_id = f"{record['id']}_{record.get('date', record.get('created_at', ''))}"
                    if composite_id not in ids:
                        ids.add(composite_id)
                    else:
                        continue
                yield record

            if not response.get('next_offset'):
                if sync_by_date and not date_sync_attempted:
                    params = {"date[after]": bookmark_date_posix, "sort_by[asc]": "date_at"}
                    sync_by_date = False
                    date_sync_attempted = True  # Mark that we've attempted date sync
                else:
                    LOGGER.info("Final offset reached. Ending sync.")
                    done = True
            else:
                params['offset'] = response.get('next_offset')
