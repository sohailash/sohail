{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0871fe74-1b15-451f-8d1e-c843623d47f4",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "ename": "ModuleNotFoundError",
     "evalue": "No module named 'pandas'",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mModuleNotFoundError\u001b[0m                       Traceback (most recent call last)",
      "Cell \u001b[0;32mIn[3], line 1\u001b[0m\n\u001b[0;32m----> 1\u001b[0m \u001b[38;5;28;01mimport\u001b[39;00m \u001b[38;5;21;01mpandas\u001b[39;00m \u001b[38;5;28;01mas\u001b[39;00m \u001b[38;5;21;01mpd\u001b[39;00m\n\u001b[1;32m      2\u001b[0m \u001b[38;5;28;01mimport\u001b[39;00m \u001b[38;5;21;01mnumpy\u001b[39;00m \u001b[38;5;28;01mas\u001b[39;00m \u001b[38;5;21;01mnp\u001b[39;00m\n\u001b[1;32m      3\u001b[0m \u001b[38;5;28;01mimport\u001b[39;00m \u001b[38;5;21;01madal\u001b[39;00m\n",
      "\u001b[0;31mModuleNotFoundError\u001b[0m: No module named 'pandas'"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import numpy as np\n",
    "import adal\n",
    "import pyodbc\n",
    "import struct\n",
    "import asyncio\n",
    "from env_canada import ECWeather\n",
    "from datetime import date\n",
    "\n",
    "from siketlrear import siket"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "eafeb476-97dd-438c-a0e8-854ad0fd1aab",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%sh\n",
    "curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -\n",
    "curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list\n",
    "sudo apt-get update\n",
    "sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "47ace2a6-db15-4f0c-abbe-c7eb82a6bdf5",
     "showTitle": false,
     "title": ""
    }
   },
   "source": [
    "##Function to check the values ( Just for Testing Purpose )##"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "19d893fb-239e-4e24-ab58-35cfca404374",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import asyncio\n",
    "from env_canada import ECWeather\n",
    "ec_en = ECWeather(station_id=\"ON/s0000786\")\n",
    "await(ec_en.update())\n",
    "result = ec_en.conditions\n",
    "print(result)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f1e3da4a-846e-4919-9b8a-5c716977038f",
     "showTitle": false,
     "title": ""
    }
   },
   "source": [
    "##Getting the values for Mississauga weather station##"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "681f8a92-619a-4dd5-adbf-22ecd4aa0384",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import adal\n",
    "import pyodbc\n",
    "import struct\n",
    "import asyncio\n",
    "from env_canada import ECWeather\n",
    "import datetime\n",
    "from datetime import timedelta\n",
    "\n",
    "\n",
    "\n",
    "async def get_weather():\n",
    "    ec_en = ECWeather(station_id=\"ON/s0000786\")\n",
    "    await ec_en.update()\n",
    "    result = ec_en.conditions\n",
    "    return result\n",
    "\n",
    "async def insert_weather_data():\n",
    "    try:\n",
    "        weather_data = await get_weather()\n",
    "\n",
    "        if not isinstance(weather_data, dict):\n",
    "            print(\"Weather data is not a dictionary!\")\n",
    "            return  # Exit the function or handle the situation accordingly\n",
    "\n",
    "        station_id='ON/s0000786'\n",
    "        station_name='Mississauga'\n",
    "        temperature = weather_data.get('temperature', {}).get('value')\n",
    "        humidity = weather_data.get('humidity', {}).get('value')\n",
    "        wind_speed = weather_data.get('wind_speed', {}).get('value')\n",
    "        visibility = weather_data.get('visibility', {}).get('value')\n",
    "        wind_direction=weather_data.get('wind_dir', {}).get('value')\n",
    "        pop=weather_data.get('pop',{}).get('value')\n",
    "        condition=weather_data.get('condition',{}).get('value')\n",
    "        wind_chill=weather_data.get('wind_chill',{}).get('value')\n",
    "        wind_bearing=weather_data.get('wind_bearing',{}).get('value')\n",
    "        high_temp=weather_data.get('high_temp',{}).get('value')\n",
    "        low_temp=weather_data.get('low_temp',{}).get('value')\n",
    "        precip_yesterday=weather_data.get('precip_yesterday',{}).get('value')\n",
    "        high_temp_yesterday=weather_data.get('high_temp_yesterday',{}).get('value')\n",
    "        low_temp_yesterday=weather_data.get('low_temp_yesterday',{}).get('value')\n",
    "        weather_observation_time=weather_data.get('observationTime', {}).get('value')\n",
    "        system_load_datetime=datetime.datetime.now()\n",
    "        wind_gust=weather_data.get('wind_gust', {}).get('value')\n",
    "        Sunrise=weather_data.get('sunrise', {}).get('value')\n",
    "        sunset=weather_data.get('sunset', {}).get('value')\n",
    "\n",
    "\n",
    "        conn = pyodbc.connect(connstr, attrs_before={1256: tokenstruct})\n",
    "        cursor = conn.cursor()\n",
    "        \n",
    "        # Verify extracted data\n",
    "        print(f\"Temperature: {temperature}, Humidity: {humidity}, Wind Speed: {wind_speed}\")\n",
    "\n",
    "        sql_query = \"INSERT INTO xxxx.xxxxxxx (station_id,station_name,temperature, humidity,visibility,condition, wind_speed,wind_gust,wind_dir,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,sunrise,sunset,observation_time,modified_datetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\"\n",
    "        values = (station_id,station_name,temperature, humidity,visibility,condition,wind_speed,wind_gust,wind_direction,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,Sunrise,sunset,weather_observation_time,system_load_datetime)\n",
    "\n",
    "        cursor.execute(sql_query, values)\n",
    "        conn.commit()\n",
    "        print(\"Data inserted successfully!\")\n",
    "    except Exception as e:\n",
    "        print(f\"Error occurred: {e}\")\n",
    "\n",
    "# Run the insertion process asynchronously\n",
    "await insert_weather_data()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e399514d-6f44-4768-b00e-217264d312ba",
     "showTitle": false,
     "title": ""
    }
   },
   "source": [
    "##Getting the values for Toronto weather station##"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "3cb33fe5-b035-4da1-a0db-f57bd20c8ee8",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import adal\n",
    "import pyodbc\n",
    "import struct\n",
    "import asyncio\n",
    "from env_canada import ECWeather\n",
    "import datetime\n",
    "from datetime import timedelta\n",
    "\n",
    "\n",
    "async def get_weather():\n",
    "    ec_en = ECWeather(station_id=\"ON/s0000458\")\n",
    "    await ec_en.update()\n",
    "    result = ec_en.conditions\n",
    "    return result\n",
    "\n",
    "async def insert_weather_data():\n",
    "    try:\n",
    "        weather_data = await get_weather()\n",
    "\n",
    "        if not isinstance(weather_data, dict):\n",
    "            print(\"Weather data is not a dictionary!\")\n",
    "            return  # Exit the function or handle the situation accordingly\n",
    "\n",
    "        station_id='ON/s0000458'\n",
    "        station_name='Toronto'\n",
    "        temperature = weather_data.get('temperature', {}).get('value')\n",
    "        humidity = weather_data.get('humidity', {}).get('value')\n",
    "        wind_speed = weather_data.get('wind_speed', {}).get('value')\n",
    "        visibility = weather_data.get('visibility', {}).get('value')\n",
    "        wind_direction=weather_data.get('wind_dir', {}).get('value')\n",
    "        pop=weather_data.get('pop',{}).get('value')\n",
    "        condition=weather_data.get('condition',{}).get('value')\n",
    "        wind_chill=weather_data.get('wind_chill',{}).get('value')\n",
    "        wind_bearing=weather_data.get('wind_bearing',{}).get('value')\n",
    "        high_temp=weather_data.get('high_temp',{}).get('value')\n",
    "        low_temp=weather_data.get('low_temp',{}).get('value')\n",
    "        precip_yesterday=weather_data.get('precip_yesterday',{}).get('value')\n",
    "        high_temp_yesterday=weather_data.get('high_temp_yesterday',{}).get('value')\n",
    "        low_temp_yesterday=weather_data.get('low_temp_yesterday',{}).get('value')\n",
    "        weather_observation_time=weather_data.get('observationTime', {}).get('value')\n",
    "        system_load_datetime=datetime.datetime.now()\n",
    "        wind_gust=weather_data.get('wind_gust', {}).get('value')\n",
    "        Sunrise=weather_data.get('sunrise', {}).get('value')\n",
    "        sunset=weather_data.get('sunset', {}).get('value')\n",
    "\n",
    "\n",
    "        conn = pyodbc.connect(connstr, attrs_before={1256: tokenstruct})\n",
    "        cursor = conn.cursor()\n",
    "        \n",
    "        # Verify extracted data\n",
    "        print(f\"Temperature: {temperature}, Humidity: {humidity}, Wind Speed: {wind_speed}\")\n",
    "\n",
    "        sql_query = \"INSERT INTO xxxx.xxxxxx (station_id,station_name,temperature, humidity,visibility,condition, wind_speed,wind_gust,wind_dir,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,sunrise,sunset,observation_time,modified_datetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\"\n",
    "        values = (station_id,station_name,temperature, humidity,visibility,condition,wind_speed,wind_gust,wind_direction,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,Sunrise,sunset,weather_observation_time,system_load_datetime)\n",
    "\n",
    "        cursor.execute(sql_query, values)\n",
    "        conn.commit()\n",
    "        print(\"Data inserted successfully!\")\n",
    "    except Exception as e:\n",
    "        print(f\"Error occurred: {e}\")\n",
    "\n",
    "# Run the insertion process asynchronously\n",
    "await insert_weather_data()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b12a6807-9d65-4ce3-97a2-b506715a403b",
     "showTitle": false,
     "title": ""
    }
   },
   "source": [
    "##Getting the values for Brampton weather station##"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a2013960-6242-45a1-971b-7226af79123b",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import adal\n",
    "import pyodbc\n",
    "import struct\n",
    "import asyncio\n",
    "from env_canada import ECWeather\n",
    "import datetime\n",
    "from datetime import timedelta\n",
    "\n",
    "\n",
    "async def get_weather():\n",
    "    ec_en = ECWeather(station_id=\"ON/s0000658\")\n",
    "    await ec_en.update()\n",
    "    result = ec_en.conditions\n",
    "    return result\n",
    "\n",
    "async def insert_weather_data():\n",
    "    try:\n",
    "        weather_data = await get_weather()\n",
    "\n",
    "        if not isinstance(weather_data, dict):\n",
    "            print(\"Weather data is not a dictionary!\")\n",
    "            return  # Exit the function or handle the situation accordingly\n",
    "\n",
    "        station_id='ON/s0000658'\n",
    "        station_name='Brampton'\n",
    "        temperature = weather_data.get('temperature', {}).get('value')\n",
    "        humidity = weather_data.get('humidity', {}).get('value')\n",
    "        wind_speed = weather_data.get('wind_speed', {}).get('value')\n",
    "        visibility = weather_data.get('visibility', {}).get('value')\n",
    "        wind_direction=weather_data.get('wind_dir', {}).get('value')\n",
    "        pop=weather_data.get('pop',{}).get('value')\n",
    "        condition=weather_data.get('condition',{}).get('value')\n",
    "        wind_chill=weather_data.get('wind_chill',{}).get('value')\n",
    "        wind_bearing=weather_data.get('wind_bearing',{}).get('value')\n",
    "        high_temp=weather_data.get('high_temp',{}).get('value')\n",
    "        low_temp=weather_data.get('low_temp',{}).get('value')\n",
    "        precip_yesterday=weather_data.get('precip_yesterday',{}).get('value')\n",
    "        high_temp_yesterday=weather_data.get('high_temp_yesterday',{}).get('value')\n",
    "        low_temp_yesterday=weather_data.get('low_temp_yesterday',{}).get('value')\n",
    "        weather_observation_time=weather_data.get('observationTime', {}).get('value')\n",
    "        system_load_datetime=datetime.datetime.now()\n",
    "        wind_gust=weather_data.get('wind_gust', {}).get('value')\n",
    "        Sunrise=weather_data.get('sunrise', {}).get('value')\n",
    "        sunset=weather_data.get('sunset', {}).get('value')\n",
    "\n",
    "\n",
    "        conn = pyodbc.connect(connstr, attrs_before={1256: tokenstruct})\n",
    "        cursor = conn.cursor()\n",
    "        \n",
    "        # Verify extracted data\n",
    "        print(f\"Temperature: {temperature}, Humidity: {humidity}, Wind Speed: {wind_speed}\")\n",
    "\n",
    "        sql_query = \"INSERT INTO xxxx.xxxxx (station_id,station_name,temperature, humidity,visibility,condition, wind_speed,wind_gust,wind_dir,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,sunrise,sunset,observation_time,modified_datetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\"\n",
    "        values = (station_id,station_name,temperature, humidity,visibility,condition,wind_speed,wind_gust,wind_direction,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,Sunrise,sunset,weather_observation_time,system_load_datetime)\n",
    "\n",
    "        cursor.execute(sql_query, values)\n",
    "        conn.commit()\n",
    "        print(\"Data inserted successfully!\")\n",
    "    except Exception as e:\n",
    "        print(f\"Error occurred: {e}\")\n",
    "\n",
    "# Run the insertion process asynchronously\n",
    "await insert_weather_data()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f0d503d5-c4c6-48fc-ab77-42bab5dd8927",
     "showTitle": false,
     "title": ""
    }
   },
   "source": [
    "##Getting the values for Hamilton weather station##"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e63e1278-7431-4bf4-839f-123e2c63049c",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import adal\n",
    "import pyodbc\n",
    "import struct\n",
    "import asyncio\n",
    "from env_canada import ECWeather\n",
    "import datetime\n",
    "from datetime import timedelta\n",
    "\n",
    "\n",
    "async def get_weather():\n",
    "    ec_en = ECWeather(station_id=\"ON/s0000549\")\n",
    "    await ec_en.update()\n",
    "    result = ec_en.conditions\n",
    "    return result\n",
    "\n",
    "async def insert_weather_data():\n",
    "    try:\n",
    "        weather_data = await get_weather()\n",
    "\n",
    "        if not isinstance(weather_data, dict):\n",
    "            print(\"Weather data is not a dictionary!\")\n",
    "            return  # Exit the function or handle the situation accordingly\n",
    "\n",
    "        station_id='ON/s0000549'\n",
    "        station_name='Hamilton'\n",
    "        temperature = weather_data.get('temperature', {}).get('value')\n",
    "        humidity = weather_data.get('humidity', {}).get('value')\n",
    "        wind_speed = weather_data.get('wind_speed', {}).get('value')\n",
    "        visibility = weather_data.get('visibility', {}).get('value')\n",
    "        wind_direction=weather_data.get('wind_dir', {}).get('value')\n",
    "        pop=weather_data.get('pop',{}).get('value')\n",
    "        condition=weather_data.get('condition',{}).get('value')\n",
    "        wind_chill=weather_data.get('wind_chill',{}).get('value')\n",
    "        wind_bearing=weather_data.get('wind_bearing',{}).get('value')\n",
    "        high_temp=weather_data.get('high_temp',{}).get('value')\n",
    "        low_temp=weather_data.get('low_temp',{}).get('value')\n",
    "        precip_yesterday=weather_data.get('precip_yesterday',{}).get('value')\n",
    "        high_temp_yesterday=weather_data.get('high_temp_yesterday',{}).get('value')\n",
    "        low_temp_yesterday=weather_data.get('low_temp_yesterday',{}).get('value')\n",
    "        weather_observation_time=weather_data.get('observationTime', {}).get('value')\n",
    "        system_load_datetime=datetime.datetime.now()\n",
    "        wind_gust=weather_data.get('wind_gust', {}).get('value')\n",
    "        Sunrise=weather_data.get('sunrise', {}).get('value')\n",
    "        sunset=weather_data.get('sunset', {}).get('value')\n",
    "\n",
    "\n",
    "        conn = pyodbc.connect(connstr, attrs_before={1256: tokenstruct})\n",
    "        cursor = conn.cursor()\n",
    "        \n",
    "        # Verify extracted data\n",
    "        print(f\"Temperature: {temperature}, Humidity: {humidity}, Wind Speed: {wind_speed}\")\n",
    "\n",
    "        sql_query = \"INSERT INTO xxxx.xxxx (station_id,station_name,temperature, humidity,visibility,condition, wind_speed,wind_gust,wind_dir,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,sunrise,sunset,observation_time,modified_datetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\"\n",
    "        values = (station_id,station_name,temperature, humidity,visibility,condition,wind_speed,wind_gust,wind_direction,wind_bearing,high_temp,low_temp,high_temp_yesterday,low_temp_yesterday,precip_yesterday,Sunrise,sunset,weather_observation_time,system_load_datetime)\n",
    "\n",
    "        cursor.execute(sql_query, values)\n",
    "        conn.commit()\n",
    "        print(\"Data inserted successfully!\")\n",
    "    except Exception as e:\n",
    "        print(f\"Error occurred: {e}\")\n",
    "\n",
    "# Run the insertion process asynchronously\n",
    "await insert_weather_data()"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "dashboards": [],
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "weather_data_extraction",
   "widgets": {}
  },
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 1
}
