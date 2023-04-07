import pandas
import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from streamstats.aggregations import generate
from streamstats.misc import parse_argument


class StreamstatsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Positional("count", required=False, otl_type=OTLType.TEXT),
            Keyword("window", required=False, otl_type=OTLType.NUMERIC),
            Keyword("time_window", required=False, otl_type=OTLType.TEXT),
            Positional("aggregation_name", required=False, otl_type=OTLType.FUNCTION, inf=True)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start streamstats command')
        # get arguments
        count = self.get_arg("count")
        window = self.get_arg("window").value  # is int
        time_window = self.get_arg("time_window").value
        # time_window may be int, like time_window=4
        # in this case it is a number of seconds, and we convert it to 4s,
        # or it may be string, like time_window='15min', so we keep it as is,
        # or it may be None
        time_window = f'{time_window}s' if time_window is not None and time_window.isdigit() else time_window

        # time_window has a priority under window
        if window and time_window:
            self.log_progress('Warning: you use window and time_window arguments, time_window only will be used.')

        # check if dataframe has a _time column
        if '_time' not in df:
            raise ValueError(f'Dataframe must have _time column.')

        # check if dataframe is sorted by _time column
        if not (df['_time'].is_monotonic_increasing or df['_time'].is_monotonic_decreasing):
            self.log_progress('Dataframe was not sorted, sorting...')
            df = df.sort_values(by='_time', inplace=True)

        # if we have both time_window and window, then we use time_window, otherwise window
        window = window if time_window is None else time_window

        # get all the aggregations
        aggregation_names = [x for x in self.get_iter("aggregation_name")]
        # if positional count is available - we must add it to all other aggregations
        if count:
            aggregation_names.append(count)

        # Main logic here
        # go through each command
        result_list = []
        for aggregation in aggregation_names:
            params = dict()
            agg_name, params['args'], named_as, params['grouped_by'] = parse_argument(aggregation)
            # make the aggregation
            temp = generate(dataframe=df, name=agg_name, named_as=named_as, window=window, **params)
            # add it to results list
            result_list.append(temp)

        # make it all be a part of the one whole dataframe
        if len(result_list) > 1:
            result = pandas.concat(result_list, axis=1)
        else:
            result = result_list[0]

        # finish | return the result
        return result
