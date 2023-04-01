import data_cleaning
import database_utils
import data_extraction

card_data = data_cleaning.DataCleaning().clean_card_data()

card_data.head()