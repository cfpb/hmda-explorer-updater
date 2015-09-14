#!/bin/bash

# This was originally all in a markdown file but I migrated it to this script by
# simply copying and pasting all the commands. There's little error handling so
# if it crashes it's probably best to go through the file step-by-step.
#
# You must define the following env variables beforehand:
#
# export CENSUS_API_KEY=XXXXXXXXXXX
# export MONGO_DEV_HOST=XXXXXXXXXXX
# export MONGO_DEV_PORT=XXXXXXXXXXX
# export MONGO_DEV_USERNAME=XXXXXXXXXXX
# export MONGO_DEV_PASSWORD=XXXXXXXXXXX
#
# Get a Census API key from: http://api.census.gov/data/key_signup.html
#
# This script is designed to run against the dev mongo server. Ask the Delivery 
# Team for its address and port. You'll also need an account that's authorized 
# to run aggregations and edit collections in the `hmda` database.
#
# You must also have GDAL, MongoDB, jq and iconv installed. On Mac OS X you can do:
#
# brew update
# brew install gdal jq mongodb
#

# Abort script if any command fails.
set -e

# Colors for terminal highlighting
color_red=$(tput setaf 1)
color_green=$(tput setaf 2)
color_yellow=$(tput setaf 3)
color_reset=$(tput sgr0)

function println_alert {
  echo "${color_red}  $1${color_reset}"
}
function println_warning {
  echo "${color_yellow}  $1${color_reset}"
}
function println_normal {
  echo "${color_green}  $1${color_reset}"
}
function print_success {
  echo "${color_green}✓${color_reset}"
  say "Success!"
}
function print_failure {
  echo "${color_red}X${color_reset}"
  say "Failure!"
  exit 1
}

# Speak out loud.
say() {
  if hash espeak 2>/dev/null; then
    echo "$@" | espeak -s 120 2>/dev/null
  elif hash /usr/bin/say 2>/dev/null; then
    /usr/bin/say "$@" 2>/dev/null
  fi
}

# Check if env variables have been set.
: ${MONGO_DEV_HOST?"You need to define MONGO_DEV_HOST"}
: ${MONGO_DEV_PORT?"You need to define MONGO_DEV_PORT"}
: ${MONGO_DEV_USERNAME?"You need to define MONGO_DEV_USERNAME"}
: ${MONGO_DEV_PASSWORD?"You need to define MONGO_DEV_PASSWORD"}

# Check if necessary tools have been installed.
programs=(ogr2ogr jq mongo mongoimport mongoexport iconv)
for i in "${programs[@]}"
do
  command -v $i >/dev/null 2>&1 || { println_alert >&2 "This script requires $i but it's not installed. Aborting."; exit 1; }
done

# Abort on ctrl + c
int_handler(){
  println_alert "Script aborted!"
  # Kill the parent process of the script.
  kill $PPID
  exit 1
}
trap 'int_handler' INT

function start {
  echo -n "  $1... "
  say "$1"
}

function check {
  if [ $1 -eq 0 ]; then
    print_success
  else
    print_failure
  fi
}

start "Creating tmp directory"
mkdir -p input/tmp
check $?

start "Creating output directory"
mkdir -p output
check $?

start "Grouping HMDA records by county. This will take a few minutes"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/hmda_group_by_county-compressed.js
check $?

start "Downloading and processing state population data from the census"
curl -s "http://api.census.gov/data/2010/sf1?key=$CENSUS_API_KEY&get=P0010001,NAME&for=state:*" | jq '.[1:] | map({population:.[0], state_name:.[1], state_code:.[2]})' > input/tmp/state-populations.json
check $?

start "Processing county population data from the census"
iconv --from-code iso-8859-1 input/census_data/PEP_2014_PEPANNRES/PEP_2014_PEPANNRES_with_ann.csv | jq --slurp --raw-input 'split("\n") | .[2:] | map(split(",")) | map({state_code: .[0][9:11], county_code: .[0][11:], county_name: (.[2]|ltrimstr("\"")), state_name: (.[3]|rtrimstr("\"")|ltrimstr(" ")), population: (.[10]|rtrimstr("\r"))})' > input/tmp/county-populations.json
check $?

start "Importing state population data into new 'state_populations' collection"
mongoimport --quiet -h $MONGO_DEV_HOST:$MONGO_DEV_PORT -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --db hmda --collection state_populations --type json --file input/tmp/state-populations.json --jsonArray --drop
check $?

start "Importing county population data into new 'county_populations' collection"
mongoimport --quiet -h $MONGO_DEV_HOST:$MONGO_DEV_PORT -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --db hmda --collection county_populations --type json --file input/tmp/county-populations.json --jsonArray --drop
check $?

start "Adding state names to county records. This will take a few minutes"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/add_state_names_to_counties.js
check $?

start "Joining HMDA records with state and county names and populations. This will take a few minutes"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/add_states_and_counties.js
check $?

start "Rounding the percent changes and adding commas to large numbers"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/make_numbers_pretty.js
check $?

# Now that you have a comprehensive `hmda_lar_by_county` collection, let's integrate the county shapefiles.

start "Downloading and unzipping county shapefiles from the Census"
curl -s http://www2.census.gov/geo/tiger/GENZ2014/shp/cb_2014_us_county_500k.zip | tar -xf- -C input/tmp
check $?

start "Converting the census' shapefile into a GeoJSON file"
rm -f input/tmp/cb_2014_us_county_500k.json
ogr2ogr -f 'GeoJSON' input/tmp/cb_2014_us_county_500k.json input/tmp/cb_2014_us_county_500k.shp
check $?

start "Cleaning up the GeoJSON file"
cat input/tmp/cb_2014_us_county_500k.json | jq '.features' --compact-output > input/tmp/counties_geojson.json
check $?

start "Importing the new 'counties_geojson.json' into a 'county_shapes' collection"
mongoimport --quiet -h $MONGO_DEV_HOST:$MONGO_DEV_PORT -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --db hmda --collection county_shapes --file input/tmp/counties_geojson.json --jsonArray --drop
check $?

start "Creating a 2dsphere spatial index on that new collection"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --eval 'db.county_shapes.ensureIndex({"geometry":"2dsphere"})' | tail -n +2
check $?

start "Joining the shapes collection with the HMDA data into a new collection. This will take a few minutes"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/add_county_shapes.js
check $?

start "Exporting the new GIS-ified collection"
mongoexport --quiet -h $MONGO_DEV_HOST:$MONGO_DEV_PORT -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --db hmda --collection hmda_lar_geo --jsonArray | jq '{"type": "FeatureCollection", "features": map({type: .type, geometry: .geometry, properties: .properties})}' --compact-output > output/hmda_lar_geo.json
check $?

if [ -f output/hmda_lar_geo.json ]; then
  # Copy over TileMill base projects
  cp -r input/tilemill_projects output/
  # Copy the generated GeoJSON file into each project dir.
  ls output/tilemill_projects/ | xargs -n 1 -I project_dir cp output/hmda_lar_geo.json output/tilemill_projects/project_dir
  println_normal "TileMill project files have been successfully generated!"
else
  println_alert "'hmda_lar_geo.json' was not successfully created. Something went wrong!"
fi 

# All done with the map stuff. Now let's generate the JSON for the charts.

start "Grouping HMDA records by state for the charts. This will take a few minutes"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/hmda_group_by_state-compressed.js
check $?

start "Add state names to the HMDA chart records"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin mongo-scripts/add_state_names_to_charts.js
check $?

start "Exporting the JSON for chart #1"
mongoexport --quiet -h $MONGO_DEV_HOST:$MONGO_DEV_PORT -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --db hmda --collection hmda_lar_by_state --jsonArray | jq 'map({(.state_name): {name: (.state_name), data: [[.purchases12, .purchases13, .purchases14], [.refinances12, .refinances13, .refinances14], [.improvements12, .improvements13, .improvements14]]}})' > input/tmp/chart1.js
check $?

start "Exporting the JSON for chart #2"
mongoexport --quiet -h $MONGO_DEV_HOST:$MONGO_DEV_PORT -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --db hmda --collection hmda_lar_by_state --jsonArray | jq 'map(select(.state_name != null) | {(.state_name): {name: (.state_name), data: [[.conv12/.purchases12*100, .conv13/.purchases13*100, .conv14/.purchases14*100], [.fha12/.purchases12*100, .fha13/.purchases13*100, .fha14/.purchases14*100], [.va12/.purchases12*100, .va13/.purchases13*100, .va14/.purchases14*100], [.rhs12/.purchases12*100, .rhs13/.purchases13*100, .rhs14/.purchases14*100]]}})' > input/tmp/chart2.js
check $?

start "Processing the JSON for both charts"
node mongo-scripts/aggregate_charts.js
check $?

if [ -f output/chart2.json ]; then
  println_normal "The JSON for chart #1 has been created and is ready to be added to hmda-explorer!"
else
  println_alert "'chart1.json' was not successfully created. Something went wrong!"
fi 

if [ -f output/chart2.json ]; then
  println_normal "The JSON for chart #2 has been created and is ready to be added to hmda-explorer!"
else
  println_alert "'chart2.json' was not successfully created. Something went wrong!"
fi 

start "Dropping all the temporary collections"
mongo --quiet $MONGO_DEV_HOST:$MONGO_DEV_PORT/hmda -u $MONGO_DEV_USERNAME -p $MONGO_DEV_PASSWORD --authenticationDatabase=admin --eval 'db.hmda_lar_by_county.drop();db.hmda_lar_by_state.drop();db.county_populations.drop();db.state_populations.drop();db.county_shapes.drop();db.hmda_lar_geo.drop();' | tail -n +2
check $?

start "Deleting tmp directory"
rm -rf input/tmp
check $?

say "Hooray! All done!"
