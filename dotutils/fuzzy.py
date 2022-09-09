from xxlimited import Str
from rapidfuzz import fuzz, process
from typing import Literal, Union, Optional
import pandas as pd
from tqdm import tqdm

tqdm.pandas()
import numpy as np


class EmptyDataFrame(Exception):
    def __init__(self, message=f"DataFrame passed is empty / has no data - {__name__}"):
        self.message = message
        super().__init__(self.message)


class MissingColumns(Exception):
    def __init__(
        self,
        message="Anchor / Target Dataframe Columns Arguments Are Not Found in DataFrames",
    ):
        self.message = message
        super().__init__(self.message)


class InvalidFuzzyAlgorithm(Exception):
    def __init__(
        self,
        message="Algorithm Passed in Settings Must be a valid member of fuzzywuzzy.fuzz",
    ):
        self.message = message
        super().__init__(self.message)


class FuzzyResult:
    def __init__(self, text=None, score=None):
        self.text = text
        self.score = score


class Fuzzy:
    """Contains a list of functions that can perform a variety of string matching operations."""

    def fuzzy_match_best(
        self,
        query: str,
        choice_list: list[str],
        algorithm: Literal[
            "partial_ratio", "ratio", "token_set_ratio", "token_sort_ratio", "wratio"
        ] = "partial_ratio",
        threshold: int = 80,
    ) -> tuple[Optional[str], Optional[int]]:
        if (
            algorithm in fuzz.__dict__.keys()
            and query
            and type(query) is str
            and choice_list
            and type(choice_list) is list
            and all([type(ch) is str for ch in choice_list])
            and type(threshold) is int
            and threshold in range(0, 101)
        ):

            choice_list_clean: list[str] = [
                choice for choice in choice_list if type(choice) is str
            ]
            choice_list = choice_list_clean

            fuzzy_result = process.extractOne(
                query=query,
                choices=choice_list,
                scorer=fuzz.__dict__[algorithm],
                score_cutoff=threshold,
            )
            if fuzzy_result:
                return fuzzy_result[0], fuzzy_result[1]

            return None, None
        else:
            raise ValueError(f"One or more function arguments are not respecting types")

    def fuzzy_all_match(
        self,
        query: str,
        choice_list: list[str],
        algorithm: Literal[
            "partial_ratio", "ratio", "token_set_ratio", "token_sort_ratio", "wratio"
        ] = "partial_ratio",
        threshold: int = 80,
    ) -> list:
        matches: list[FuzzyResult] = []
        if (
            algorithm in fuzz.__dict__.keys()
            and query
            and type(query) is str
            and choice_list
            and type(choice_list) is list
            and all([type(ch) is str for ch in choice_list])
            and type(threshold) is int
            and threshold in range(0, 101)
        ):

            for choice in choice_list:
                match_result = fuzz.__dict__[algorithm](query, choice)
                if match_result and match_result >= threshold:
                    matches.append(FuzzyResult(choice, match_result))

            return matches
        else:
            raise ValueError(f"One or more function arguments are not respecting types")

    def fuzzy_join(
        self,
        anchor_df: pd.DataFrame,
        anchor_column: str,
        target_df: pd.DataFrame,
        target_column: str,
        threshold: int = 80,
        algorithm: Literal[
            "partial_ratio", "ratio", "token_set_ratio", "token_sort_ratio", "wratio"
        ] = "partial_ratio",
        display_columns: list[str] = [],
        approach: Literal["best_match", "all_match"] = "best_match",
    ) -> pd.DataFrame:
        """Left Join Two Tables (Target DF on Anchor DF) by a String Column but by matching at a fuzzy level

        Args:
            anchor_df (pd.DataFrame): Left Table of Left Fuzzy Join
            anchor_column (str): Column of Left Table to Match with Another Column / Table
            target_df (pd.DataFrame): Right Table of Left Fuzzy Join
            target_column (str): Column of Right Table to Match with Column of Left Table
            algorithm (fuzz, optional): Fuzzywuzzy scorer. Defaults to fuzz.partial_ratio.
            threshold (int, optional): A number below which even best matches can be disregarded. Defaults to 80.
            display_columns (list, optional): Include additional columns from target_df along with fuzzy matched result
            approach (str, optional): 'best_match` matches one against each row, and `all_match` returns all possible matches within threshold

        Raises:
            EmptyDataFrame: DataFrame Objects to Join are empty.
            MissingColumns: Columns Passed to Fuzzy Match do not exist in DataFrame or are not String Types.
            InvalidFuzzyAlgorithm: Algorithm Passes is not a Valid FuzzyWuzzy Algorithm
            MismatchedInput: Threshold Passed Incorrectly

        Returns:
            pd.DataFrame: Left Joined Table with 2 new columns : Fuzzy Match Result, and Score of Respective Match
        """

        fuzzy_approach = dict(
            best_match=self.fuzzy_match_best,
            all_match=self.fuzzy_all_match,
        )

        if anchor_df.empty or target_df.empty:
            raise EmptyDataFrame

        if (
            anchor_column not in anchor_df.columns
            or target_column not in target_df.columns
            or anchor_df[anchor_column].dtype != np.dtype("O")
            or target_df[target_column].dtype != np.dtype("O")
        ):
            # check if string columns are present in the dataframes
            raise MissingColumns

        if approach not in fuzzy_approach.keys():
            # check if scorer is a valid fuzzywuzzy algorithm
            raise InvalidFuzzyAlgorithm(
                "Fuzzy Approach mentioned is invalid. Use one of `best_match` or `all_match`"
            )

        # Pass every row of left table into fuzzy matching function
        if approach == "best_match":

            def __fuzzy_match_best_conditional(row, target_df_copy) -> list:
                return_list = []
                fuzzy_result: tuple = self.fuzzy_match_best(
                    query=row.loc[anchor_column],
                    choice_list=target_df_copy[target_column].unique(),
                    algorithm=algorithm,
                    threshold=threshold,
                )

                fuzzy_result_nan = [res if res else np.nan for res in fuzzy_result]
                return_list.extend(fuzzy_result_nan)

                if display_columns:
                    disp_col_list = [np.nan for _ in range(len(display_columns))]
                    # filter dataframe for result
                    target_df_filt = target_df_copy[
                        target_df_copy[target_column] == fuzzy_result[0]
                    ]
                    target_df_filt = target_df_filt.reset_index(drop=True)
                    if not target_df_filt.empty:
                        for col_index, col_name in enumerate(display_columns):
                            cell_value = target_df_filt[col_name][0]
                            disp_col_list[col_index] = cell_value

                    return_list.extend(disp_col_list)

                return return_list

            new_anchor_columns = [
                f"f_{anchor_column}_{target_column}",
                f"f_score_{anchor_column}_{target_column}",
            ]
            display_columns_formatted = [
                f"target_{col_name}" for col_name in display_columns
            ]
            new_anchor_columns.extend(display_columns_formatted)

            anchor_df[new_anchor_columns] = anchor_df.apply(
                lambda row: __fuzzy_match_best_conditional(row, target_df.copy()),
                axis=1,
                result_type="expand",
            )

        elif approach == "all_match":
            anchor_df[f"f_{anchor_column}_{target_column}"] = anchor_df.progress_apply(
                lambda row: self.fuzzy_all_match(
                    query=row.loc[anchor_column],
                    choice_list=target_df[target_column].unique(),
                    algorithm=algorithm,
                    threshold=threshold,
                ),
                axis=1,
            )

        # returns even if no fuzzy matching / pandas operational exceptions are raised
        return anchor_df

    def window_fuzzy_join(
        self,
        anchor_df: pd.DataFrame,
        anchor_column: str,
        target_df: pd.DataFrame,
        target_column: str,
        window_col: str,
        threshold: int = 80,
        algorithm: str = "partial_ratio",
        display_columns: list = [],
        approach="best_match",
    ) -> pd.DataFrame:
        """Contextual Fuzzy Join. Fuzzy Matching is only done for subset of both dataframes that belong to window_col values.
        The window_col must be present in both anchor and target dataframes.

        Args:
            anchor_df (pd.DataFrame): The left table to join
            anchor_column (str): The column in anchor_df to query across target column.
            target_df (pd.DataFrame): The right table to join
            target_column (str): The column in target_df where values from anchor_df are compared against
            window_col (str): The contextual column within which to independently fuzzy match
            algorithm (fuzz, optional): Fuzzywuzzy scorer. Defaults to fuzz.partial_ratio.
            threshold (int, optional): A number below which even best matches can be disregarded. Defaults to 80.
            display_columns (list, optional): Include additional columns from target_df along with fuzzy matched result
            approach (str, optional): 'best_match` matches one against each row, and `all_match` returns all possible matches within threshold

        Returns:
            pd.DataFrame: The anchor dataframe with fuzzy matched values within window
        """
        if window_col in anchor_df.columns and window_col in target_df.columns:

            columns_in_window: list[str] = list(anchor_df.columns).extend(
                [
                    f"f_{anchor_column}_{target_column}",
                    f"f_score_{anchor_column}_{target_column}",
                ]
            )
            fuzzy_df: pd.DataFrame = pd.DataFrame(data=[], columns=columns_in_window)
            window_values: np.ndarray = anchor_df[window_col].unique()

            for window in tqdm(window_values):
                window_anchor_df: pd.DataFrame = anchor_df[
                    anchor_df[window_col] == window
                ]
                window_target_df: pd.DataFrame = target_df[
                    target_df[window_col] == window
                ]

                if not window_target_df.empty:
                    window_fuzzy_df = self.fuzzy_join(
                        anchor_df=window_anchor_df,
                        anchor_column=anchor_column,
                        target_df=window_target_df,
                        target_column=target_column,
                        threshold=threshold,
                        algorithm=algorithm,
                        display_columns=display_columns,
                        approach=approach,
                    )
                else:
                    window_fuzzy_df = window_anchor_df

                fuzzy_df = pd.concat([fuzzy_df, window_fuzzy_df]).reset_index(drop=True)

            return fuzzy_df
