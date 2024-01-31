import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Daily_Raw_Flights_Data
Daily_Raw_Flights_Data_node1706603828380 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airlines",
        table_name="daily_raw_data",
        transformation_ctx="Daily_Raw_Flights_Data_node1706603828380",
    )
)

# Script generated for node dim_airport_code_Read
dim_airport_code_Read_node1706603860626 = glueContext.create_dynamic_frame.from_catalog(
    database="airlines",
    table_name="dev_airlines_airports_dim",
    redshift_tmp_dir="s3://temp-bucket-for-redshift",
    transformation_ctx="dim_airport_code_Read_node1706603860626",
)

# Script generated for node OriginId_Join_AirportId
OriginId_Join_AirportId_node1706604075300 = Join.apply(
    frame1=Daily_Raw_Flights_Data_node1706603828380,
    frame2=dim_airport_code_Read_node1706603860626,
    keys1=["originairportid"],
    keys2=["airport_id"],
    transformation_ctx="OriginId_Join_AirportId_node1706604075300",
)

# Script generated for node Arrival_Airport_Schema_Changes
Arrival_Airport_Schema_Changes_node1706604406797 = ApplyMapping.apply(
    frame=OriginId_Join_AirportId_node1706604075300,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("depdelay", "long", "dep_delay", "long"),
        ("arrdelay", "long", "arr_delay", "long"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
    ],
    transformation_ctx="Arrival_Airport_Schema_Changes_node1706604406797",
)

# Script generated for node DepartureId_Join AirportId
DepartureId_JoinAirportId_node1706604616438 = Join.apply(
    frame1=Arrival_Airport_Schema_Changes_node1706604406797,
    frame2=dim_airport_code_Read_node1706603860626,
    keys1=["destairportid"],
    keys2=["airport_id"],
    transformation_ctx="DepartureId_JoinAirportId_node1706604616438",
)

# Script generated for node Departure_Airport_Schema_Changes
Departure_Airport_Schema_Changes_node1706604780211 = ApplyMapping.apply(
    frame=DepartureId_JoinAirportId_node1706604616438,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("state", "string", "arr_state", "string"),
        ("arr_delay", "long", "arr_delay", "long"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_delay", "long", "dep_delay", "long"),
        ("dep_airport", "string", "dep_airport", "string"),
    ],
    transformation_ctx="Departure_Airport_Schema_Changes_node1706604780211",
)

# Script generated for node Write_into_Redshift_fact_table
Write_into_Redshift_fact_table_node1706605172330 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=Departure_Airport_Schema_Changes_node1706604780211,
        database="airlines",
        table_name="dev_airlines_daily_flights_fact",
        redshift_tmp_dir="s3://temp-bucket-for-redshift",
        additional_options={
            "aws_iam_role": "arn:aws:iam::730335270820:role/redshift-role"
        },
        transformation_ctx="Write_into_Redshift_fact_table_node1706605172330",
    )
)

job.commit()
