import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measurement = Base.classes.measurement
station= Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
  """List all available api routes."""
  return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation,<br/>"
        f"/api/v1.0/stations<br/>"    
        f"/api/v1.0/tobs,<br/>"
        f"/api/v1.0/start&gt;/&lt;end&gt"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    
    # connect to session
    session = Session(engine)
    
    # query dates and precipitation values
    precipitation_df = session.query(measurement.date, measurement.prcp).all()
    
    # convert the query results to a dictionary using date as the key and prcp as the value
    precipitation_df = {} 
    for date, prcp in precipitation_df:
        precipitation_df[date] = prcp
    
    session.close()

    return jsonify(precipitation_df)

# Route: /api/v1.0/stations
# Return a JSON list of stations from the dataset
@app.route("/api/v1.0/stations")
def stations():
    
    # connect to session
    session = Session(engine)
    
    # create dictionary
    stations = {}

    # Query all stations. Convert the query results to a dictionary using station as the key and name as the value
    stations_count = session.query(func.count, stations.id).all()
    for station, name in stations_count:
        stations[station] = name

    session.close()
 
    return jsonify(stations)

# Route: /api/v1.0/tobs
# Query the dates and temperature observations of the most active station for the last year of data.
# Return a JSON list of temperature observations (TOBS) for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():
    
    # connect to session
    session = Session(engine)
    
    # find most active station
    active_station_count = session.query(measurement.station, func.count(measurement.station)).group_by(measurement.station).\
                            order_by(func.count(measurement.station).desc()).first()[0]
    
    # get the last date and date from one year ago in the dataset
    lastDate = session.query(measurement.date).order_by(measurement.date.desc()).first()
    query_date = (dt.datetime.strptime(lastDate[0], '%Y-%m-%d') - dt.timedelta(days=365)).strftime('%Y-%m-%d')
    
    # query the dates and tobs based on the most active station
    active_station_query = session.query(measurement.date, measurement.tobs).\
                            filter((measurement.station == active_station_count)\
                            & (measurement.date >= query_date)\
                            & (measurement.date <= lastDate)).all()
    
    session.close()
 
    return jsonify(tobs)

# Route: /api/v1.0/<start>/<end>
# Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start-end range.
# calculate the `TMAX`, `TMIN`, and `TAVG` for dates between the start and end date.
@app.route("/api/v1.0/<start>/<end>")
def temp_range(start, end):
    
    # connect to session
    session = Session(engine)
    
    start_end_list = []

    start_end_list = session.query(measurement.date, func.min(measurement.tobs),\
                       func.avg(measurement.tobs), func.max(measurement.tobs)).\
                       filter(measurement.date >= start).\
                       filter(measurement.date <= end).\
                       group_by(measurement.date).all()
    # Create a dictionary from the row data and append to a list
    
    for date, min, avg, max in start_end_list:
        start_end_dict = {}
        start_end_dict["Date"] = date
        start_end_dict["TMaX"] = max
        start_end_dict["TMIN"] = min
        start_end_dict["TAVG"] = avg
        start_end_list.append(start_end_dict)

        session.close()

    return jsonify(start_end_list)


if __name__ == '__main__':
    app.run(debug=True)
