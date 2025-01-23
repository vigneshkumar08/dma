import requests
import pandas as pd
import psycopg2


def dms(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        extracted_data = [
            {
                "FinancialyearID":item["FinancialyearID"],
                "GroupingName":item["GroupingName"],
                "RegionName":item["RegionName"],
                "DistrictName":item["DistrictName"],
                "MuncipalityName":item["MuncipalityName"],
                "GMName":item["GMName"],
                "Date":item["Date"],
                "TotalAssesments":item["TotalAssesments"],
                "TotalDemand":item["TotalDemand"],
                "TotalCollection":item["TotalCollection"],
                "TotalBal":item["TotalBal"],
                "%Collection":item["%Collection"]
            }
            for item in data
        ]
        return pd.DataFrame(extracted_data)
    else:
        raise Exception(f"Failed to fetch data. Status Code: {response.status_code}")


def csv(df, file_name):
    df.to_csv(file_name, index=False)
    print(f"Data successfully saved to {file_name}")

def normalize_name(name):
    """
    Normalize the district name for consistent comparison.
    """
    if name is None:
        return None
    name = name.strip().lower()  
    

    corrections = {
        "tiruchirappalli": "thiruchirappalli",
        "kanyakumari": "kanniyakumari",
 
    }
    
    return corrections.get(name, name)  



def create_table(cursor, df):
    """
    Create the `dma_tax` table if it doesn't exist and insert data into it.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dma_tax(
       "FinancialyearID" VARCHAR(100),
       "GroupingName" VARCHAR(100),
       "RegionName" VARCHAR(100),
       "DistrictName" VARCHAR(100),
       "MuncipalityName" VARCHAR(100),
       "GMName" VARCHAR(100),
       "Date" VARCHAR(100),
       "TotalAssesments" NUMERIC,
       "TotalDemand" NUMERIC,
       "TotalCollection" NUMERIC,
       "TotalBal" NUMERIC,
       "%Collection" NUMERIC
    );
    """
    cursor.execute(create_table_query)

    for _, row in df.iterrows():
        district_name_trimmed = normalize_name(row['DistrictName'])
        print(f"Normalized district name: {district_name_trimmed}")

        # Fetch district_id with normalized names
        cursor.execute(
            """
            SELECT dis_id FROM district_id 
            WHERE LOWER(TRIM(district_name)) = %s
            """,
            (district_name_trimmed,)
        )
        district_id_result = cursor.fetchone()

        if district_id_result is not None:
            district_id = district_id_result[0]
            print(f"Match found: {district_name_trimmed} -> District ID: {district_id}")
        else:
            print(f"No match found for district name: '{row['DistrictName']}' (normalized: '{district_name_trimmed}')")
            continue  # Skip to the next iteration if district_id is not found


        insert_query = """
        INSERT INTO dma_tax ("FinancialyearID", "GroupingName", "RegionName", "DistrictName", "MuncipalityName", "GMName", "Date", 
        "TotalAssesments", "TotalDemand", "TotalCollection", "TotalBal", "Collection")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        data_tuple = (
            row["FinancialyearID"],
            row["GroupingName"],
            row["RegionName"],
            district_id,
            row["MuncipalityName"],
            row["GMName"],
            row["Date"],
            row["TotalAssesments"],
            row["TotalDemand"],
            row["TotalCollection"],
            row["TotalBal"],
            row["%Collection"]
        )
        cursor.execute(insert_query, data_tuple)



def main():
    url = "https://tnurbanepay.tn.gov.in/api/WW_Dashboard/CM_dashboard_DCB?orgtype=6"
    db_config = {
        "dbname": "test",
        "user": "admin",
        "password": "godspeed123",
        "host": "192.168.50.26",
        "port": "5431"
    }

    try:
        df = dms(url)
        csv(df, 'dma_tax.csv')

        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

       
    except Exception as e:
        print("An error occurred:", e)

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()
