import cdsapi

client = cdsapi.Client()

dataset = "satellite-land-cover"
request = {
    "variable": "all",
    "year": ["2020"],
    "version": ["v2_1_1"]
}
target = "/data_2/scratch/ting/data_raw/CCI_landcover_2020/land_cover_2020.nc"

client.retrieve(dataset, request, target)

