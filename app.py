import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify, render_template

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///DataSets/belly_button_biodiversity.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to the tables
otu_table = Base.classes.otu
samples = Base.classes.samples
metadata_s = Base.classes.samples_metadata

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/names")
def names():
    sample_names = session.query(samples).statement
    df = pd.read_sql_query(sample_names, session.bind)
    df.set_index('otu_id', inplace=True)

    return jsonify(list(df.columns))

@app.route("/otu")
def otu():
    otu_query = session.query(otu_table.lowest_taxonomic_unit_found).all()
    otu_json = np.ravel(otu_query)

    return jsonify(list(otu_json))

@app.route("/metadata/<sample>")
def metadata(sample):
    results = session.query(metadata_s.AGE, metadata_s.BBTYPE, metadata_s.ETHNICITY, 
    metadata_s.GENDER, metadata_s.LOCATION, metadata_s.SAMPLEID).filter(metadata_s.SAMPLEID == sample).all()

    metadata_dict= {}
    for result in results: 
            metadata_dict["AGE"] = result[0]
            metadata_dict["BBTYPE"] = result[1]
            metadata_dict["ETHNICITY"] = result[2]
            metadata_dict["GENDER"] = result[3]
            metadata_dict["LOCATION"] =  result[4]
            metadata_dict["SAMPLEID"] = result[5]
        
    return jsonify(metadata_dict)

@app.route("/wfreq/<sample>")
def wfreq(sample):
    results = session.query(metadata_s.WFREQ).filter(metadata_s.SAMPLEID == sample).all()
    wfreq_int = results[0][0]

    return jsonify(wfreq_int)

@app.route('/samples/<sample>')
def sample_otu(sample):
    query = session.query(samples).statement
    otu_df = pd.read_sql_query(query, session.bind)

    otu_df = otu_df.sort_values(by=sample, ascending=0)

    data = [{
        "otu_ids": otu_df[sample].index.values.tolist(),
        "sample_values": otu_df[sample].values.tolist()
    }]

    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)