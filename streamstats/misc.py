from typing import Union

from otlang.sdk.argument import Tree, Argument


def parse_argument(argument: Union[Tree, Argument]) -> tuple:
    if isinstance(argument, Tree):
        name = argument.value['funcname']['value']
        args = [x['value'] for x in argument.value['funcargs']]
        named_as = name if argument.value.get('named_as') is None else argument.value['named_as']['value']
        grouped_by = [x['value'] for x in argument.value['grouped_by']]
        return name, args, named_as, grouped_by

    if isinstance(argument, Argument):
        name = argument.value
        args = argument.key
        named_as = argument.named_as
        grouped_by = [x for x in argument.group_by]
        return name, args, named_as, grouped_by

    raise ValueError(f'Only Argument and Tree types are supported, {type(argument)} is given instead')
