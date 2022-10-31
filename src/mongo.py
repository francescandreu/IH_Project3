import pandas as pd
import numpy as np
from pymongo import MongoClient


# ------------------------------------------ SETUP ------------------------------------------
def connectToMongo():
    client = MongoClient("localhost:27017")
    db = client["Ironhack"]
    c = db.get_collection("companies")
    return c


# ------------------------------------------ GET ------------------------------------------
def getCompaniesAndLocation(c, category):
    filter_ = {"category_code":category}
    projection = {'category_code':1, 'name':1, 'offices.city':1, 'offices.country_code':1, 'offices.latitude':1, 'offices.longitude':1, '_id':0}
    result = list(c.find(filter_, projection))
    return result

def getTopCountry(base_df):
    countriesOrdered = base_df.groupby('Country').count()
    return countriesOrdered.sort_values(by=['Company'], ascending=False).index[0]

def getTop3Cities(base_df, country):
    df_topCountry = base_df.loc[base_df['Country']==country]
    df_topCountry = df_topCountry.sort_values(by=['City']).replace(to_replace='None', value=np.nan).dropna()
    return list(df_topCountry.groupby('City').count().sort_values(by=['Company'], ascending=False).index[:3])

def getCompanies_TagCityCountry(c, tag, country, city):
    filter_ = {'tag_list':{'$regex':tag}, 'offices.city':city, 'offices.country_code':country}
    projection = {'name':1, '_id':0}
    result = list(c.find(filter_, projection))
    return len(result)

# ------------------------------------------ BUILD ------------------------------------------
def buildBaseDF(companies):
    col_names = ['Company', 'City', 'Country', 'lat', 'lon']
    df = pd.DataFrame(data=companies, columns=col_names)
    return df

def buildTopCitiesDF(base_df, country, cities):
    df = base_df.loc[(base_df['Country']==country) &
                 ((base_df['City']==cities[0]) | (base_df['City']==cities[1]) | (base_df['City']==cities[2]))]
    return df.sort_values(by=['City'])

def buildDesignCompanies(index, row, main_df, df_cities, cities):
    if(row.City == cities[0]):
        main_df.loc[index, 'Design Companies'] = df_cities.loc[0, 'Design Companies']
    elif (row.City == cities[1]):
        main_df.loc[index, 'Design Companies'] = df_cities.loc[1, 'Design Companies']
    elif (row.City == cities[2]):
        main_df.loc[index, 'Design Companies'] = df_cities.loc[2, 'Design Companies']
    return main_df

def valueLocation(row):
    design = row['Design Companies'] * (20/87) * 0.2
    club = row['Nightclub'] * (87/87) * 0.1
    eSchool = row['Elementary School'] * (26/87)
    mSchool = row['Middle School'] * (26/87)
    hSchool = row['High School'] * (26/87)
    school = (eSchool+mSchool+hSchool) * 0.25
    starbucks = row['Starbucks'] * (10/87) * 0.1
    airport = row['Airport'] * (20/87)
    train = row['Train'] * (20/87)
    transport = (airport+train) * 0.2
    basketball = row['Basketball'] * (1/87) * 0.05
    vegan = row['Vegan'] * (1/87) * 0.05
    pet = row['Pet'] * (1/87) * 0.05

    return design + club + school + starbucks + transport + basketball + vegan + pet


# ------------------------------------------ CLEAN ------------------------------------------
def unflatten_result(result):
    companies = []
    for company in result:
        aux = []
        name = company['name']
        location = [value for element in list(company['offices']) for key,value in element.items()]
        location.insert(0,name)
        companies.append(location[:5])
    return companies

