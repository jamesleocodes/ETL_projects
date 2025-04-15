import unittest
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
import pandas as pd
import yaml
import sys
import os

sys.path.append("../src")  # Adjust path as needed
from etl_person import (
    extract_from_csv,
    extract_from_json,
    extract_from_xml,
    transform,
    load_data,
    log_progress,
)


class TestETLPerson(unittest.TestCase):
    def setUp(self):
        # Sample expected DataFrame
        self.expected_df = pd.DataFrame(
            {
                "name": ["John", "Alice"],
                "height": [70, 65],  # In inches
                "weight": [150, 120],  # In pounds
            }
        )

        self.transformed_df = pd.DataFrame(
            {
                "name": ["John", "Alice"],
                "height": [1.78, 1.65],  # Converted to meters
                "weight": [68.04, 54.43],  # Converted to kilograms
            }
        )

    @patch(
        "pandas.read_csv",
        return_value=pd.DataFrame(
            {"name": ["John", "Alice"], "height": [70, 65], "weight": [150, 120]}
        ),
    )
    def test_extract_from_csv(self, mock_read_csv):
        result = extract_from_csv("dummy.csv")
        pd.testing.assert_frame_equal(result, self.expected_df)

    @patch(
        "pandas.read_json",
        return_value=pd.DataFrame(
            {"name": ["John", "Alice"], "height": [70, 65], "weight": [150, 120]}
        ),
    )
    def test_extract_from_json(self, mock_read_json):
        result = extract_from_json("dummy.json")
        pd.testing.assert_frame_equal(result, self.expected_df)

    @patch("xml.etree.ElementTree.parse")
    def test_extract_from_xml(self, mock_parse):
        # Mock each <person> element
        mock_person_1 = MagicMock()
        mock_person_1.find.side_effect = lambda tag: MagicMock(
            text="John" if tag == "name" else "70" if tag == "height" else "150"
        )
        mock_person_2 = MagicMock()
        mock_person_2.find.side_effect = lambda tag: MagicMock(
            text="Alice" if tag == "name" else "65" if tag == "height" else "120"
        )

        # Mock the root element to return a list of <person> elements
        mock_root = MagicMock()
        mock_root.__iter__.return_value = [mock_person_1, mock_person_2]

        # Mock the tree and its root
        mock_tree = MagicMock()
        mock_tree.getroot.return_value = mock_root
        mock_parse.return_value = mock_tree

        # Run the extract_from_xml function
        result = extract_from_xml("dummy.xml")

        # Compare the result to the expected DataFrame
        pd.testing.assert_frame_equal(result, self.expected_df)

    def test_transform(self):
        result = transform(self.expected_df)
        pd.testing.assert_frame_equal(result, self.transformed_df)

    @patch("pandas.DataFrame.to_csv")
    def test_load_data(self, mock_to_csv):
        load_data("dummy_target.csv", self.transformed_df)
        mock_to_csv.assert_called_once_with("dummy_target.csv", index=False)

    @patch("builtins.open", new_callable=mock_open)
    def test_log_progress(self, mock_file):
        message = "Test message"
        log_progress(message)
        mock_file().write.assert_called_once_with(
            f"{datetime.now().strftime('%Y-%h-%d-%H:%M:%S')} - {message}\n"
        )


if __name__ == "__main__":
    unittest.main()
