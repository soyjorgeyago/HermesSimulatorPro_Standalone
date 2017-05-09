package es.us.lsi.hermes.util;

import es.us.lsi.hermes.google.directions.*;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.openStreetMap.PositionSimulatedSpeed;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoutesUtils {

    public static void createPathOpenStreetMaps(List<PositionSimulatedSpeed> pssList, LocationLog ll) {
        if (pssList == null || pssList.isEmpty())
            return;

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
            if (currentCoordinates == null || currentCoordinates.isEmpty() || currentCoordinates.size() < 2)
                continue;

            // Creamos un nodo del trayecto, como si usásemos SmartDriver.
            LocationLogDetail lld = new LocationLogDetail(ll, currentCoordinates.get(1), currentCoordinates.get(0), pss.getSpeed());

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
            lld.setSecondsToBeHere(pathDurationInSeconds);

            locationLogDetailList.add(lld);

            // Asignamos el actual al anterior, para poder seguir calculando las distancias y tiempos respecto al punto previo.
            previous = pss;
        }

        // Asignamos el listado de posiciones.
        ll.setLocationLogDetailList(locationLogDetailList);

        ll.setDistance(pathDistance);
        ll.setDuration(pathDurationInSeconds);

    }

    public static void createPathGoogleMaps(GeocodedWaypoints gcwp, LocationLog ll) {

        // Analizamos la información obtenida de la consulta a Google Directions.
        // Nuestra petición sólo devolverá una ruta.
        if (gcwp.getRoutes() == null || gcwp.getRoutes().isEmpty())
            return;

        Route r = gcwp.getRoutes().get(0);
        // Comprobamos que traiga información de la ruta.
        if (r.getLegs() == null)
            return;

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
            // Creamos un nodo del trayecto, como si usásemos SmartDriver.
            LocationLogDetail lld = new LocationLogDetail(ll, location.getLat(), location.getLng(), null);

            // Calculamos la distancia en metros entre los puntos previo y actual, así como el tiempo necesario para recorrer dicha distancia.
            Double pointDistance = Util.distanceHaversine(previous.getLat(), previous.getLng(), location.getLat(), location.getLng());
            pathDistance += pointDistance;
            // Calculamos el tiempo en segundos que tarda en recorrer la distancia entre los puntos.
            int pointDuration = (int) Math.ceil(l.getDuration().getValue() * pointDistance / l.getDistance().getValue());

            // Convertimos la velocidad a Km/h.
            speed = pointDuration > 0 ? pointDistance * 3.6 / pointDuration : 0.0d;
            lld.setSpeed(speed);

            // Añadimos los segundos correspondientes a la distancia recorrida entre puntos.
            pathDurationInSeconds += pointDuration;
            // Indicamos cuántos segundos deben pasar para estar en esta posición.
            lld.setSecondsToBeHere(pathDurationInSeconds);

            locationLogDetailList.add(lld);

            // Asignamos el actual al anterior, para poder seguir calculando las distancias y tiempos respecto al punto previo.
            previous = location;
        }

        // Asignamos el listado de posiciones.
        ll.setLocationLogDetailList(locationLogDetailList);

        ll.setDistance(pathDistance);
        ll.setDuration(pathDurationInSeconds);
    }

    public static Location getRandomLocation(double latitude, double longitude, int radius, Logger LOG) {
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
}