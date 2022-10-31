import os
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
from cartoframes.viz import Map, Layer, popup_element

import mongo as mn
import foursquare as fs

def showDF(df):
    print("..........................................................................")
    print(df)
    print("..........................................................................")


def main():
    load_dotenv()
    c = mn.connectToMongo()

    if (os.path.exists('data\mainDF2.csv')):
        print("File already exists.")
        main_df = pd.read_csv('data\mainDF2.csv')
    else:
        print("File not found, creating new DF.")
        result = mn.getCompaniesAndLocation(c, 'games_video')
        companies = mn.unflatten_result(result)
        base_df = mn.buildBaseDF(companies)

        country = mn.getTopCountry(base_df)
        cities = mn.getTop3Cities(base_df, country)
        main_df = mn.buildTopCitiesDF(base_df, country, cities)
        
        list_count_design_companies = []
        for city in cities:
            num_design_companies = mn.getCompanies_TagCityCountry(c, '(design)', country, city)
            list_count_design_companies.append([city, num_design_companies])
        df_cities = pd.DataFrame(list_count_design_companies, columns=['City', 'Design Companies'])

        for index, row in main_df.iterrows():
            # Design companies in city
            main_df = mn.buildDesignCompanies(index, row, main_df, df_cities, cities)
            
            # Distance to NIGHTCLUB
            distance = fs.getClosesVenue('Nightclub', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Nightclub')

            # Distance to ELEMENTARY SCHOOL
            distance = fs.getClosesVenue('Elementary School', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Elementary School')

            # Distance to MIDDLE SCHOOL
            distance = fs.getClosesVenue('Middle School', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Middle School')

            # Distance to HIGH SCHOOL
            distance = fs.getClosesVenue('High School', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'High School')

            # Distance to STARBUCKS
            distance = fs.getClosesVenue('starbucks', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Starbucks')

            # Distance to AIRPORT
            distance = fs.getClosesVenue('airport', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Airport')

            # Distance to TRAIN STATION 
            distance = fs.getClosesVenue('Train Station', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Train')

            # Distance to BASKETBALL COURT
            distance = fs.getClosesVenue('Basketball Court', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Basketball')

            # Distance to VEGAN RESTAURANT
            distance = fs.getClosesVenue('Vegetarian / Vegan Restaurant', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Vegan')

            # Distance to PET SERVICE
            distance = fs.getClosesVenue('Pet Service', [row.lon, row.lat])
            main_df = fs.addDistanceInfo(main_df, distance, index, 'Pet') 

        main_df = main_df.dropna()
        for index, row in main_df.iterrows():
            main_df.loc[index, 'Value'] = mn.valueLocation(row)

        main_df = main_df.sort_values(by=['Value'])
        main_df.to_csv ('data\mainDF2.csv', index = None, header=True) 


    chosenOffice = main_df.iloc[0]

    # Get coordinates in needed format:
    #       Using FourSquare -> Get office location
    #                        -> Get near venues
    # Create gdf
    # Generate Map

if __name__ == '__main__':
    main()