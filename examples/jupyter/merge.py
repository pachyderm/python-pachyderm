import os
import csv
import json
import datetime

PRICE = 5

def main():
    try:
        weather_filenames = os.listdir("/pfs/weather")
    except:
        weather_filenames = []

    with open("/pfs/out/data.csv", "w") as out_file:
        writer = csv.writer(out_file)

        for weather_filename in weather_filenames:
            dt = datetime.datetime.strptime(weather_filename, "%Y-%m-%d")
            trip_filepath = "/pfs/trips/{}-{}".format(dt.month, dt.strftime("%d-%y"))

            if os.path.exists(trip_filepath):
                with open("/pfs/weather/{}".format(weather_filename), "r") as weather_file:
                    with open(trip_filepath, "r") as trip_file:
                        weather_json = json.load(weather_file)
                        precip = weather_json["daily"]["data"][0]["precipProbability"]

                        trip_csv = csv.reader(trip_file)
                        next(trip_csv) # skip the header row
                        trips = int(next(trip_csv)[1])

                        writer.writerow([dt.strftime("%Y-%m-%d"), precip, trips, trips * PRICE])

if __name__ == "__main__":
    main()
