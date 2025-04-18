import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

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
        # Expected DataFrame after extraction, including car_model
        self.expected_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.0, 12000.0],
                "fuel": ["Petrol", "Diesel"],
                "car_model": ["Baleno", None],  # One record has car_model, one doesn't
            }
        ).astype(
            {
                "year_of_manufacture": "int64",
                "price": "float64",
                "fuel": "object",
                "car_model": "object",
            }
        )

        # Expected DataFrame after transformation (NaN in car_model -> "Unknown")
        self.transformed_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.00, 12000.00],  # Rounded to 2 decimals
                "fuel": ["Petrol", "Diesel"],
                "car_model": ["Baleno", "Unknown"],
            }
        ).astype(
            {
                "year_of_manufacture": "int64",
                "price": "float64",
                "fuel": "object",
                "car_model": "object",
            }
        )

    def test_extract_from_csv(self):
        # Mock CSV data with car_model
        mock_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.0, 12000.0],
                "fuel": ["Petrol", "Diesel"],
                "car_model": ["Baleno", None],
            }
        )
        with patch("pandas.read_csv", return_value=mock_df):
            result = extract_from_csv("dummy.csv")
            pd.testing.assert_frame_equal(result, self.expected_df)

    def test_extract_from_csv_missing_car_model(self):
        # Mock CSV data without car_model column
        mock_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.0, 12000.0],
                "fuel": ["Petrol", "Diesel"],
            }
        )
        with patch("pandas.read_csv", return_value=mock_df):
            result = extract_from_csv("dummy.csv")
            expected = self.expected_df.copy()
            expected["car_model"] = None
            pd.testing.assert_frame_equal(result, expected)

    def test_extract_from_json(self):
        # Mock JSON data with car_model
        mock_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.0, 12000.0],
                "fuel": ["Petrol", "Diesel"],
                "car_model": ["Baleno", None],
            }
        )
        with patch("pandas.read_json", return_value=mock_df):
            result = extract_from_json("dummy.json")
            pd.testing.assert_frame_equal(result, self.expected_df)

    def test_extract_from_json_missing_car_model(self):
        # Mock JSON data without car_model column
        mock_df = pd.DataFrame(
            {
                "year_of_manufacture": [2020, 2021],
                "price": [10000.0, 12000.0],
                "fuel": ["Petrol", "Diesel"],
            }
        )
        with patch("pandas.read_json", return_value=mock_df):
            result = extract_from_json("dummy.json")
            expected = self.expected_df.copy()
            expected["car_model"] = None
            pd.testing.assert_frame_equal(result, expected)

    @patch("xml.etree.ElementTree.parse")
    def test_extract_from_xml(self, mock_parse):
        # Mock XML structure for year_of_manufacture, price, fuel, and car_model
        mock_car_1 = MagicMock()
        mock_car_1.find.side_effect = lambda tag: (
            MagicMock(text="2020") if tag == "year_of_manufacture" else
            MagicMock(text="10000") if tag == "price" else
            MagicMock(text="Petrol") if tag == "fuel" else
            MagicMock(text="Baleno") if tag == "car_model" else None
        )

        mock_car_2 = MagicMock()
        mock_car_2.find.side_effect = lambda tag: (
            MagicMock(text="2021") if tag == "year_of_manufacture" else
            MagicMock(text="12000") if tag == "price" else
            MagicMock(text="Diesel") if tag == "fuel" else
            None if tag == "car_model" else None
        )

        # Mock the root and its children
        mock_root = MagicMock()
        mock_root.__iter__.return_value = [mock_car_1, mock_car_2]
        mock_tree = MagicMock()
        mock_tree.getroot.return_value = mock_root
        mock_parse.return_value = mock_tree

        # Run the function
        result = extract_from_xml("dummy.xml")

        # Compare the result with the expected DataFrame
        pd.testing.assert_frame_equal(result, self.expected_df)

    @patch("xml.etree.ElementTree.parse")
    def test_extract_from_xml_error(self, mock_parse):
        # Test error handling when XML parsing fails
        mock_parse.side_effect = Exception("Invalid XML")
        result = extract_from_xml("dummy.xml")
        expected = pd.DataFrame(
            columns=["year_of_manufacture", "price", "fuel", "car_model"]
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_transform(self):
        # Test transformation: price rounding and car_model NaN handling
        result = transform(self.expected_df.copy())
        pd.testing.assert_frame_equal(result, self.transformed_df)

    def test_transform_all_missing_car_model(self):
        # Test transformation when all car_model values are missing
        input_df = self.expected_df.copy()
        input_df["car_model"] = None
        result = transform(input_df)
        expected = self.transformed_df.copy()
        expected["car_model"] = "Unknown"
        pd.testing.assert_frame_equal(result, expected)

    @patch("pandas.DataFrame.to_csv")
    def test_load_data(self, mock_to_csv):
        # Test that load_data calls to_csv with the correct arguments
        load_data("dummy_target.csv", self.transformed_df)
        mock_to_csv.assert_called_once_with("dummy_target.csv", index=False)

if __name__ == "__main__":
    unittest.main()
