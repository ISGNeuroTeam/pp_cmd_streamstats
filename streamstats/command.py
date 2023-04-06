import pandas
import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from streamstats.aggregations import generate


class StreamstatsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("window", required=False, otl_type=OTLType.NUMERIC),
            Keyword("time_window", required=False, otl_type=OTLType.NUMERIC),
            Positional("aggregation_name", required=False, otl_type=OTLType.FUNCTION, inf=True)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # self.log_progress('Start streamstats command')
        # that is how you get arguments
        window = self.get_arg("window").value
        time_window = self.get_arg("time_window").value

        if window and time_window:
            self.log_progress('Warning: you use window and time_window arguments, time_window only will be used.')

        # check if dataframe has a _time column
        if '_time' not in df:
            raise ValueError(f'Dataframe must have _time column.')

        # check if dataframe is sorted by _time column
        if not (df['_time'].is_monotonic_increasing or df['_time'].is_monotonic_decreasing):
            self.log_progress('Dataframe was not sorted, sorting...')
            df = df.sort_values(by='_time', inplace=True)

        window = window if time_window is None else f'{time_window}s'

        # get all the aggregations
        aggregation_names = [x for x in self.get_iter("aggregation_name")]

        # Make your logic here
        # go through each command
        result_list = []
        for aggregation in aggregation_names:
            params = dict()
            agg_name = aggregation.value['funcname']['value']
            params['args'] = [x['value'] for x in aggregation.value['funcargs']]
            named_as = agg_name if aggregation.value.get('named_as') is None else aggregation.value['named_as']['value']
            params['grouped_by'] = [x['value'] for x in aggregation.value['grouped_by']]
            temp = generate(dataframe=df, name=agg_name, named_as=named_as, window=window, **params)
            result_list.append(temp)

        if len(result_list) > 1:
            result = pandas.concat(result_list, axis=1)
        else:
            result = result_list[0]

        return result
