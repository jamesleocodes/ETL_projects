import unittest
from unittest.mock import patch, mock_open, MagicMock
import pandas as pd
import sys
import os
sys.path.append("../src")
# Add the 'src' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.etl_car import (
    extract_from_csv,
    extract_from_json,
    extract_from_xml,
    transform,
    load_data,
)


class TestETL(unittest.TestCase):
    def setUp(self):
        # Expected DataFrame after XML extraction
        self.expected_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.0, 12000.0],
                "fuel": ["Petrol", "Diesel"],
            }
        )

    @patch("builtins.open", new_callable=mock_open, read_data="dummy_config_content")
    def test_extract_from_csv(self, mock_file):
        with patch("pandas.read_csv", return_value=self.expected_df):
            result = extract_from_csv("dummy.csv")
            pd.testing.assert_frame_equal(result, self.expected_df)

    @patch("builtins.open", new_callable=mock_open, read_data="dummy_config_content")
    def test_extract_from_json(self, mock_file):
        with patch("pandas.read_json", return_value=self.expected_df):
            result = extract_from_json("dummy.json")
            pd.testing.assert_frame_equal(result, self.expected_df)

    @patch("xml.etree.ElementTree.parse")
    def test_extract_from_xml(self, mock_parse):
        # Mock the XML structure
        mock_person_1 = MagicMock()
        mock_person_1.find.side_effect = lambda tag: (
            MagicMock(text="2020")
            if tag == "year_of_manufacture"
            else MagicMock(text="10000") if tag == "price" else MagicMock(text="Petrol")
        )

        mock_person_2 = MagicMock()
        mock_person_2.find.side_effect = lambda tag: (
            MagicMock(text="2021")
            if tag == "year_of_manufacture"
            else MagicMock(text="12000") if tag == "price" else MagicMock(text="Diesel")
        )

        # Mock the root and its children (person elements)
        mock_root = MagicMock()
        mock_root.__iter__.return_value = [mock_person_1, mock_person_2]

        # Mock the tree and its root
        mock_tree = MagicMock()
        mock_tree.getroot.return_value = mock_root
        mock_parse.return_value = mock_tree

        # Run the function
        result = extract_from_xml("dummy.xml")

        # Compare the result with the expected DataFrame
        pd.testing.assert_frame_equal(result, self.expected_df)

    def test_transform(self):
        result = transform(self.expected_df)
        self.expected_df["price"] = self.expected_df["price"].round(2)
        pd.testing.assert_frame_equal(result, self.expected_df)

    @patch("pandas.DataFrame.to_csv")
    def test_load_data(self, mock_to_csv):
        load_data("dummy_target.csv", self.expected_df)
        mock_to_csv.assert_called_once()


if __name__ == "__main__":
    unittest.main()
