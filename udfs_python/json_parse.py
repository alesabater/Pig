# import for json
from com.xhaus.jyson import JysonCodec as json



#  The pig schema that must return the script
@outputSchema("b:bag{tup:tuple(id_equipo:chararray,longitud:double,latitud:double,gps:chararray)}")
def tsvify(line):
  payload = json.loads(line)[0]
  try:
	outBag = []
        longitud = payload['location']['lon']
        latitud = payload['location']['lat']
        id_equipo = payload['id']
        gps = payload['date']['transmit']
        tup = (str(id_equipo), longitud, latitud, str(gps))
	outBag.append(tup)
        return outBag
  except:
        valores = 'ERROR AL PARSEAR MENSAJE'
  return valores


