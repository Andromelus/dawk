@include "src/master.awk"
@include "src/logging.awk"
@include "src/math.awk"
@include "src/io.awk"
# awk -v MODE="local" -v NBR_WORKERS=4 -v SHUFFLE_KEY=1 -v MAPPER="map.awk" -v REDUCER="reduce.awk" -v RED_FS="," -v MAP_FS=" " -f user.awk data 

# awk -v MODE="local" 
# -v NBR_WORKERS=4 
# -v SHUFFLE_KEY=1 
# -v MAPPER="map.awk" 
# -v REDUCER="reduce.awk" 
# -v RED_FS="," 
# -v MAP_FS=" " 
# -f user.awk 
# data 
