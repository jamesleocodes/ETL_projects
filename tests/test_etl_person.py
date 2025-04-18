import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append("../src")  # Retain if necessary for imports
# Add the 'src' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.etl_person import (
    extract_from_csv,
    extract_from_json,
    extract_from_xml,
    transform,
    load_data
)

class TestETLPerson(unittest.TestCase):
    def setUp(self):
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

    def test_extract_from_csv(self):
        with patch("pandas.read_csv", return_value=self.expected_df):
            result = extract_from_csv("dummy.csv")
            pd.testing.assert_frame_equal(result, self.expected_df)

    def test_extract_from_json(self):
        with patch("pandas.read_json", return_value=self.expected_df):
            result = extract_from_json("dummy.json")
            pd.testing.assert_frame_equal(result, self.expected_df)

    @patch("xml.etree.ElementTree.parse")
    def test_extract_from_xml(self, mock_parse):
        mock_person_1 = MagicMock()
        mock_person_1.find.side_effect = lambda tag: (
            MagicMock(text="John") if tag == "name" else
            MagicMock(text="70") if tag == "height" else
            MagicMock(text="150") if tag == "weight" else None
        )
        mock_person_2 = MagicMock()
        mock_person_2.find.side_effect = lambda tag: (
            MagicMock(text="Alice") if tag == "name" else
            MagicMock(text="65") if tag == "height" else
            MagicMock(text="120") if tag == "weight" else None
        )
        mock_root = MagicMock()
        mock_root.__iter__.return_value = [mock_person_1, mock_person_2]
        mock_tree = MagicMock()
        mock_tree.getroot.return_value = mock_root
        mock_parse.return_value = mock_tree

        result = extract_from_xml("dummy.xml")
        result = transform(result)
        pd.testing.assert_frame_equal(result, self.transformed_df)

    def test_transform(self):
        result = transform(self.expected_df)
        self.transformed_df["weight"] = self.transformed_df["weight"].round(2)
        pd.testing.assert_frame_equal(result, self.transformed_df)

    @patch("pandas.DataFrame.to_csv")
    def test_load_data(self, mock_to_csv):
        load_data("dummy_target.csv", self.expected_df)
        mock_to_csv.assert_called_once()

if __name__ == "__main__":
    unittest.main()
