package es.us.lsi.hermes.util;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import es.us.lsi.hermes.google.directions.*;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.openStreetMap.PositionSimulatedSpeed;
import es.us.lsi.hermes.simulator.PathRequestWebService;
import es.us.lsi.hermes.simulator.PresetSimulation;
import es.us.lsi.hermes.simulator.SimulatorController;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PathUtils {

    private static final Logger LOG = Logger.getLogger(PathUtils.class.getName());

    private static void createPathOpenStreetMaps(List<PositionSimulatedSpeed> pssList, LocationLog ll) {
        if (pssList == null || pssList.isEmpty()) {
            return;
        }

        // Listado de posiciones que componen el trayecto de SmartDriver.
        ArrayList<LocationLogDetail> locationLogDetailList = new ArrayList<>();

        double pathDistance = 0.0d;
        int pathDurationInSeconds = 0;

        // Posición anterior en el trayecto.
        PositionSimulatedSpeed previous = pssList.get(0);

        // Analizamos la información obtenida de la consulta a OpenStreetMap.
        for (PositionSimulatedSpeed pss : pssList) {
            List<Double> currentCoordinates = pss.getPosition().getCoordinates();
            // Comprobamos que vengan las coordenadas.
            if (currentCoordinates == null || currentCoordinates.isEmpty() || currentCoordinates.size() < 2) {
                continue;
            }

            // Creamos un nodo del trayecto, como si usásemos SmartDriver.
            LocationLogDetail lld = new LocationLogDetail(currentCoordinates.get(1), currentCoordinates.get(0), pss.getSpeed().intValue());

            List<Double> previousCoordinates = previous.getPosition().getCoordinates();
            // Calculamos la distancia en metros entre los puntos previo y actual, así como el tiempo necesario para recorrer dicha distancia.
            Double pointDistance = Util.distanceHaversine(previousCoordinates.get(1), previousCoordinates.get(0), currentCoordinates.get(1), currentCoordinates.get(0));
            pathDistance += pointDistance;

            // Convertimos los Km/h en m/s.
            double currentSpeedMS = lld.getSpeed() / 3.6d;

            // Añadimos los segundos correspondientes a la distancia recorrida entre puntos.
            int pointDuration = (int) Math.ceil(pointDistance / currentSpeedMS);
            // Añadimos los segundos correspondientes a la distancia recorrida entre puntos.
            // Indicamos cuántos segundos deben pasar para estar en esta posición.
            pathDurationInSeconds += pointDuration;
            lld.setSecondsToRemainHere(pointDuration);

            locationLogDetailList.add(lld);

            // Asignamos el actual al anterior, para poder seguir calculando las distancias y tiempos respecto al punto previo.
            previous = pss;
        }

        // Asignamos el listado de posiciones.
        ll.setLocationLogDetailList(locationLogDetailList);

        ll.setDistance(pathDistance);
        ll.setDuration(pathDurationInSeconds);
    }

    private static void createPathGoogleMaps(GeocodedWaypoints gcwp, LocationLog ll) {

        // Analizamos la información obtenida de la consulta a Google Directions.
        // Nuestra petición sólo devolverá una ruta.
        if (gcwp.getRoutes() == null || gcwp.getRoutes().isEmpty()) {
            return;
        }

        Route r = gcwp.getRoutes().get(0);
        // Comprobamos que traiga información de la ruta.
        if (r.getLegs() == null) {
            return;
        }

        Leg l = r.getLegs().get(0);

        // Listado de posiciones que componen el trayecto de SmartDriver.
        ArrayList<LocationLogDetail> locationLogDetailList = new ArrayList<>();

        double speed;
        double pathDistance = 0.0d;
        int pathDurationInSeconds = 0;

        ArrayList<Location> locationList = PolylineDecoder.decodePoly(r.getOverviewPolyline().getPoints());
        // Posición anterior en el trayecto.
        Location previous = locationList.get(0);

        // FIXME: ¿Interpolación de velocidades? Otra opción es consultar a Google Distance Matrix para consultar el tiempo que se tarda entre 2 puntos (le afecta el tráfico) y sacar la velocidad.
//                PolynomialFunction p = new PolynomialFunction(new double[]{speed, averagePolylineSpeed,});
        for (Location location : locationList) {

            // Calculamos la distancia en metros entre los puntos previo y actual, así como el tiempo necesario para recorrer dicha distancia.
            Double pointDistance = Util.distanceHaversine(previous.getLat(), previous.getLng(), location.getLat(), location.getLng());
            pathDistance += pointDistance;
            // Calculamos el tiempo en segundos que tarda en recorrer la distancia entre los puntos.
            int pointDuration = (int) Math.ceil(l.getDuration().getValue() * pointDistance / l.getDistance().getValue());

            // Convertimos la velocidad a Km/h.
            speed = pointDuration > 0 ? pointDistance * 3.6 / pointDuration : 0.0d;

            // Creamos un nodo del trayecto, como si usásemos SmartDriver.
            LocationLogDetail lld = new LocationLogDetail(location.getLat(), location.getLng(), (int) speed);

            // Añadimos los segundos correspondientes a la distancia recorrida entre puntos.
            pathDurationInSeconds += pointDuration;
            // Indicamos cuántos segundos deben pasar para estar en esta posición.
            lld.setSecondsToRemainHere(pointDuration);

            locationLogDetailList.add(lld);

            // Asignamos el actual al anterior, para poder seguir calculando las distancias y tiempos respecto al punto previo.
            previous = location;
        }

        // Asignamos el listado de posiciones.
        ll.setLocationLogDetailList(locationLogDetailList);
        ll.setDistance(pathDistance);
        ll.setDuration(pathDurationInSeconds);
    }

    private static Location getRandomLocation(double latitude, double longitude, int radius) {
        Random random = new Random();

        // TODO: Comprobar que es una localización que no sea 'unnamed'
        // El radio se considerará en kilómetros. Lo convertimos a grados.
        double radiusInDegrees = radius / 111f;

        double u = random.nextDouble();
        double v = random.nextDouble();
        double w = radiusInDegrees * Math.sqrt(u);
        double t = 2 * Math.PI * v;
        double x = w * Math.cos(t);
        double y = w * Math.sin(t);

        double new_x = x / Math.cos(latitude);

        double foundLongitude = new_x + longitude;
        double foundLatitude = y + latitude;

        LOG.log(Level.FINE, "getRandomLocation() - Longitud: {0}, Latitud: {1}", new Object[]{foundLongitude, foundLatitude});

        Location result = new Location();
        result.setLat(foundLatitude);
        result.setLng(foundLongitude);

        return result;
    }

    public static void generateSimulatedPaths() {
        // Lista con las tareas de petición de rutas.
        List<Callable<String>> pathRequestTaskList = new ArrayList<>();

        // Crearemos tantas tareas como trayectos se quieran generar.
        for (int i = 0; i < PresetSimulation.getPathsAmount(); i++) {
            final Location destination = PathUtils.getRandomLocation(Constants.SEVILLE.getLat(), Constants.SEVILLE.getLng(), PresetSimulation.getDistanceFromCenter());
            final Location origin = PathUtils.getRandomLocation(destination.getLat(), destination.getLng(), PresetSimulation.getMaxPathDistance());

            // Tarea para la petición de un trayecto.
            Callable<String> callable = new Callable<String>() {
                @Override
                public String call() throws Exception {
                    String jsonPath = null;
                    Location o = origin;
                    Location d = destination;
                    while (jsonPath == null) {
                        try {
                            if (PresetSimulation.getPathsGenerationMethod() == Constants.Paths_Generation_Method.GOOGLE.ordinal()) {
                                /////////////////
                                // GOOGLE MAPS //
                                /////////////////

                                jsonPath = IOUtils.toString(new URL("https://maps.googleapis.com/maps/api/directions/json?origin=" + o.getLat() + "," + o.getLng() + "&destination=" + d.getLat() + "," + d.getLng()), "UTF-8");
                            } else if (PresetSimulation.getPathsGenerationMethod() == Constants.Paths_Generation_Method.OPENSTREETMAP.ordinal()) {
                                ///////////////////
                                // OPENSTREETMAP //
                                ///////////////////

                                jsonPath = IOUtils.toString(new URL("http://cronos.lbd.org.es/hermes/api/smartdriver/network/simulate?fromLat=" + o.getLat() + "&fromLng=" + o.getLng() + "&toLat=" + d.getLat() + "&toLng=" + d.getLng() + "&speedFactor=1.0"), "UTF-8");
                            }
                        } catch (IOException ex) {
                            String method = Constants.Paths_Generation_Method.values()[PresetSimulation.getPathsGenerationMethod()].name();
                            LOG.log(Level.SEVERE, "generateSimulatedPaths() - {1} - Error I/O: {0}", new Object[]{ex.getMessage(), method});
                            // Generamos nuevos puntos aleatorios hasta que sean aceptados.
                            o = PathUtils.getRandomLocation(Constants.SEVILLE.getLat(), Constants.SEVILLE.getLng(), PresetSimulation.getDistanceFromCenter());
                            d = PathUtils.getRandomLocation(origin.getLat(), origin.getLng(), PresetSimulation.getMaxPathDistance());
                        }
                    }
                    return jsonPath;
                }
            };

            // Añadimos la tarea al listado de peticiones.
            pathRequestTaskList.add(callable);
        }

        // Tomamos la marca de tiempo actual. Nos servirá para espaciar las peticiones de trayectos a Google, ya que no se pueden hacer más de 10 peticiones por segundo con la cuenta gratuita.
        // Aplicamos el mismo criterio para OpenStreetMap, aunque no sea necesario en principio.
        long timeMark = System.currentTimeMillis();
        //TODO Review changes with commit a634226 just in case
        // Ejecutamos el listado de tareas, que se dividirá en los hilos y con las condiciones que haya configurados en 'PathRequestWebService'.
        for (int i = Constants.REQUEST_PACK_SIZE; i <= pathRequestTaskList.size(); i += Constants.REQUEST_PACK_SIZE) {
            long elapsedTime = System.currentTimeMillis() - timeMark;
            if (elapsedTime < 1500) {
                try {
                    // Antes de hacer la siguiente petición, esperamos 1,5 segundos, para cumplir las restricciones de Google.
                    Thread.sleep(1500 - elapsedTime);
                } catch (InterruptedException ex) {
                    // Login not necessary
                } finally {
                    timeMark = System.currentTimeMillis();
                }
            }
            requestPaths(pathRequestTaskList.subList(i - Constants.REQUEST_PACK_SIZE, i));
        }
        int remaining = pathRequestTaskList.size() % Constants.REQUEST_PACK_SIZE;
        if (remaining != 0) {
            try {
                // Antes de hacer la siguiente petición, esperamos 1 segundo, para cumplir las restricciones de Google.
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // Login not necessary
            }
            requestPaths(pathRequestTaskList.subList(pathRequestTaskList.size() - remaining, pathRequestTaskList.size()));
        }

        // Paramos el 'listener'
        PathRequestWebService.shutdown();
    }

    private static void requestPaths(List<Callable<String>> pathRequestTaskSublist) {
        try {
            // Clearing directory before saving CSV files
            StorageUtils.clearFolderContent(CSVUtils.PERMANENT_FOLDER_DRIVERS);
            StorageUtils.clearFolderContent(CSVUtils.PERMANENT_FOLDER_PATHS);

            List<Future<String>> futureTaskList = PathRequestWebService.submitAllTask(pathRequestTaskSublist);
            int pathCounter = 0;
            for (Future<String> aFutureTaskList : futureTaskList) {
                // Creamos un objeto de localizaciones de 'SmartDriver'.
                LocationLog ll = new LocationLog();

                // Procesamos el JSON de respuesta, en función de la plataforma a la que le hayamos hecho la petición.
                try {
                    String json = aFutureTaskList.get();

                    if (PresetSimulation.getPathsGenerationMethod() == Constants.Paths_Generation_Method.GOOGLE.ordinal()) {
                        /////////////////
                        // GOOGLE MAPS //
                        /////////////////

                        // Procesamos el JSON obtenido de Google Maps para crear una trayectoria de SmartDriver.
                        Gson gson = new GsonBuilder()
                                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                                .create();
                        GeocodedWaypoints gcwp = gson.fromJson(json, GeocodedWaypoints.class);
                        PathUtils.createPathGoogleMaps(gcwp, ll);
                    } else {
                        ///////////////////
                        // OPENSTREETMAP //
                        ///////////////////

                        // Procesamos el JSON obtenido de OpenStreetMap con las localizaciones y las velocidades de SmartDriver.
                        Type listType = new TypeToken<ArrayList<PositionSimulatedSpeed>>() {
                        }.getType();
                        List<PositionSimulatedSpeed> pssList = new Gson().fromJson(json, listType);
                        PathUtils.createPathOpenStreetMaps(pssList, ll);
                    }
                } catch (InterruptedException | ExecutionException | JsonSyntaxException ex) {
                    LOG.log(Level.SEVERE, "Error al decodificar el JSON de la ruta", ex);
                }

                // Si no fuera un trayecto válido, lo ignoramos y pasamos al siguiente
                if (ll.getLocationLogDetailList() == null || ll.getLocationLogDetailList().isEmpty()) {
                    continue;
                }

                // Interpolate to assure a position for each simulation second.
                ll.setLocationLogDetailList(interpolateLocationLogDetailList(ll.getLocationLogDetailList()));

                SimulatorController.getLocationLogList().add(ll);

                // RDL: Once a full route is created, store it on routes folder
                CSVUtils.createRouteDataFile(String.valueOf(pathCounter = pathCounter + 1), ll.getLocationLogDetailList());
            }
        } catch (InterruptedException ex) {
            LOG.log(Level.SEVERE, "Error obtaining the path's JSON", ex);
        }
    }

    private static List<LocationLogDetail> interpolateLocationLogDetailList(List<LocationLogDetail> lldList) {
        List<LocationLogDetail> interpolatedLocationLogDetailList = new ArrayList<>();

        for (int i = 0; i < lldList.size() - 1; i++) {
            LocationLogDetail lld1 = lldList.get(i),
                    lld2 = lldList.get(i + 1);
            interpolatedLocationLogDetailList.addAll(PathUtils.interpolateBetween(lld1, lld2));
        }

        return interpolatedLocationLogDetailList;
    }

    private static List<LocationLogDetail> interpolateBetween(LocationLogDetail lld1, LocationLogDetail lld2) {
        List<LocationLogDetail> lldListBetween = new ArrayList<>();

        // To get 1 intermediate point each second.
        int numberOfInnerLocations = lld2.getSecondsToRemainHere() - lld1.getSecondsToRemainHere();

        double latitudeFragment = (lld2.getLatitude() - lld1.getLatitude()) / numberOfInnerLocations;
        double longitudeFragment = (lld2.getLongitude() - lld1.getLongitude()) / numberOfInnerLocations;
        double speedFragment = (double) (lld2.getSpeed() - lld1.getSpeed()) / numberOfInnerLocations;

        for (int i = 0; i < numberOfInnerLocations; i++) {
            LocationLogDetail lld = new LocationLogDetail();

            lld.setLatitude(i * latitudeFragment + lld1.getLatitude());
            lld.setLongitude(i * longitudeFragment + lld1.getLongitude());
            lld.setSpeed((int) (i * speedFragment + lld1.getSpeed()));
            lld.setSecondsToRemainHere(1);

            lldListBetween.add(lld);
        }
        lld2.setSecondsToRemainHere(1);
        lldListBetween.add(lld2);

        return lldListBetween;
    }
}
