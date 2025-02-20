from typing import Any, Dict, List


class MapValidator:
    """Validator for SQL data mapping logic"""

    OPERATORS = {
        "eq": lambda x, y: x == y,
        "gt": lambda x, y: x > y,
        "lt": lambda x, y: x < y,
        "gte": lambda x, y: x >= y,
        "lte": lambda x, y: x <= y,
        "ne": lambda x, y: x != y,
    }

    @classmethod
    def evaluate_condition(cls, condition: dict, row_data: dict) -> bool:
        """Evaluate a single condition or group of conditions.

        Args:
            condition: Condition dictionary with operator and conditions/values
            row_data: Row data to validate against

        Returns:
            bool: True if conditions are met, False otherwise

        Raises:
            ValueError: If condition structure is invalid
        """
        if "operator" not in condition:
            raise ValueError("Missing operator in condition")

        if condition["operator"] in cls.OPERATORS:
            # Simple condition
            if "column" not in condition or "value" not in condition:
                raise ValueError("Simple condition must have column and value")

            column_value = row_data.get(condition["column"])
            return cls.OPERATORS[condition["operator"]](
                column_value, condition["value"]
            )

        elif condition["operator"] in ["and", "or"]:
            # Compound condition
            if "conditions" not in condition:
                raise ValueError(
                    f"{condition['operator'].upper()} operator must have conditions"
                )

            results = [
                cls.evaluate_condition(subcond, row_data)
                for subcond in condition["conditions"]
            ]
            return all(results) if condition["operator"] == "and" else any(results)

        raise ValueError(f"Unknown operator: {condition['operator']}")

    @classmethod
    def validate(cls, row_data: Dict[str, Any], map_logic: dict) -> bool:
        """Validate if row matches the map logic conditions.

        Args:
            row_data: Dictionary containing row data
            map_logic: Map logic configuration to check against

        Returns:
            bool: True if row matches conditions, False otherwise

        Raises:
            ValueError: If map logic structure is invalid
        """
        try:
            return cls.evaluate_condition(map_logic, row_data)
        except Exception as e:
            print(f"Error validating map logic: {e}")
            return False

    @staticmethod
    def _check_and_conditions(
        row_dict: Dict[str, Any], conditions: List[Dict[str, Any]]
    ) -> bool:
        """Check if all conditions in AND group match

        Args:
            row_dict: Dictionary containing row data
            conditions: List of AND conditions to check

        Returns:
            bool: True if all conditions match
        """
        return all(
            row_dict.get(condition["column_name"]) == condition["value"]
            for condition in conditions
        )

    @staticmethod
    def _check_or_conditions(
        row_dict: Dict[str, Any], conditions: List[Dict[str, Any]]
    ) -> bool:
        """Check if any condition in OR group matches

        Args:
            row_dict: Dictionary containing row data
            conditions: List of OR conditions to check

        Returns:
            bool: True if any condition matches
        """
        return any(
            row_dict.get(condition["column_name"]) == condition["value"]
            for condition in conditions
        )
