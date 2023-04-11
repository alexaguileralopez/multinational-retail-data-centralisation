import data_cleaning
import pandas as pd



card_data = data_cleaning.DataCleaning().clean_card_data()

card_data.head()