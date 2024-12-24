package ceng.ceng351.carpoolingdb;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CarPoolingSystem implements ICarPoolingSystem {

    private static String url = "jdbc:h2:mem:carpoolingdb;DB_CLOSE_DELAY=-1"; // In-memory database
    private static String user = "sa";          // H2 default username
    private static String password = "";        // H2 default password

    private Connection connection;

    public void initialize(Connection connection) {
        this.connection = connection;
    }

    // Given: getAllDrivers()
    // All Drivers after Updating the Ratings
    @Override
    public Driver[] getAllDrivers() {
        List<Driver> drivers = new ArrayList<>();

        String query = "SELECT PIN, rating FROM Drivers ORDER BY PIN ASC;";

        try {
            PreparedStatement ps = this.connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int PIN = rs.getInt("PIN");
                double rating = rs.getDouble("rating");

                // Create a Driver object with only PIN and rating
                Driver driver = new Driver(PIN, rating);
                drivers.add(driver);
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return drivers.toArray(new Driver[0]);
    }


    // Create tables
    @Override
    public int createTables() {
        int tableCount = 0;

        String createParticipants =
                "CREATE TABLE Participants (" +
                        "    PIN INT PRIMARY KEY," +
                        "    p_name VARCHAR(50)," +
                        "    age INT" +
                        ")";

        String createPassengers =
                "CREATE TABLE Passengers (" +
                        "    PIN INT PRIMARY KEY," +
                        "    membership_status VARCHAR(20)," +
                        "    FOREIGN KEY (PIN) REFERENCES Participants(PIN)" +
                        ")";

        String createDrivers =
                "CREATE TABLE Drivers (" +
                        "    PIN INT PRIMARY KEY," +
                        "    rating DOUBLE," +
                        "    FOREIGN KEY (PIN) REFERENCES Participants(PIN)" +
                        ")";

        String createCars =
                "CREATE TABLE Cars (" +
                        "    CarID INT PRIMARY KEY," +
                        "    PIN INT," +
                        "    color VARCHAR(20)," +
                        "    brand VARCHAR(20)," +
                        "    FOREIGN KEY (PIN) REFERENCES Drivers(PIN)" +
                        ")";

        String createTrips =
                "CREATE TABLE Trips (" +
                        "    TripID INT PRIMARY KEY," +
                        "    CarID INT," +
                        "    date VARCHAR(20)," +
                        "    departure VARCHAR(50)," +
                        "    destination VARCHAR(50)," +
                        "    num_seats_available INT," +
                        "    FOREIGN KEY (CarID) REFERENCES Cars(CarID)" +
                        ")";

        String createBookings =
                "CREATE TABLE Bookings (" +
                        "    TripID INT," +
                        "    PIN INT," +
                        "    booking_status VARCHAR(20)," +
                        "    PRIMARY KEY (TripID, PIN)," +
                        "    FOREIGN KEY (TripID) REFERENCES Trips(TripID)," +
                        "    FOREIGN KEY (PIN) REFERENCES Passengers(PIN)" +
                        ")";

        String[] createTableStatements = {
                createParticipants,
                createPassengers,
                createDrivers,
                createCars,
                createTrips,
                createBookings
        };

        try {
            for (String statement : createTableStatements) {
                PreparedStatement preparedStatement = this.connection.prepareStatement(statement);
                preparedStatement.executeUpdate();
                preparedStatement.close();
                tableCount++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tableCount;
    }

    // Drop tables
    @Override
    public int dropTables() {
        int droppedCount = 0;

        String[] tablesToDrop = {
                "Bookings",
                "Trips",
                "Cars",
                "Drivers",
                "Passengers",
                "Participants"
        };

        for (int i = 0; i < tablesToDrop.length; i++) {
            tablesToDrop[i] = "DROP TABLE IF EXISTS " + tablesToDrop[i];
        }

        try {
            for (String dropStatement : tablesToDrop) {
                PreparedStatement preparedStatement = this.connection.prepareStatement(dropStatement);
                preparedStatement.executeUpdate();
                preparedStatement.close();
                droppedCount++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return droppedCount;
    }



    // Insert Participants
    @Override
    public int insertParticipants(Participant[] participants) {
        int rowsInserted = 0;
        String insertSQL = "INSERT INTO Participants (PIN, p_name, age) VALUES (?, ?, ?)";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(insertSQL);

            for (Participant participant : participants) {
                preparedStatement.setInt(1, participant.getPIN());
                preparedStatement.setString(2, participant.getP_name());
                preparedStatement.setInt(3, participant.getAge());

                rowsInserted += preparedStatement.executeUpdate();
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }


    // Insert Passengers
    @Override
    public int insertPassengers(Passenger[] passengers) {
        int rowsInserted = 0;
        String insertSQL = "INSERT INTO Passengers (PIN, membership_status) VALUES (?, ?)";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(insertSQL);

            for (Passenger passenger : passengers) {
                preparedStatement.setInt(1, passenger.getPIN());
                preparedStatement.setString(2, passenger.getMembership_status());

                rowsInserted += preparedStatement.executeUpdate();
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }


    // Insert Drivers
    @Override
    public int insertDrivers(Driver[] drivers) {
        int rowsInserted = 0;
        String insertSQL = "INSERT INTO Drivers (PIN, rating) VALUES (?, ?)";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(insertSQL);

            for (Driver driver : drivers) {
                preparedStatement.setInt(1, driver.getPIN());
                preparedStatement.setDouble(2, driver.getRating());

                rowsInserted += preparedStatement.executeUpdate();
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }


    // Insert Cars
    @Override
    public int insertCars(Car[] cars) {
        int rowsInserted = 0;
        String insertSQL = "INSERT INTO Cars (CarID, PIN, color, brand) VALUES (?, ?, ?, ?)";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(insertSQL);

            for (Car car : cars) {
                preparedStatement.setInt(1, car.getCarID());
                preparedStatement.setInt(2, car.getPIN());
                preparedStatement.setString(3, car.getColor());
                preparedStatement.setString(4, car.getBrand());

                rowsInserted += preparedStatement.executeUpdate();
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }

    @Override
    public int insertTrips(Trip[] trips) {
        int rowsInserted = 0;
        String insertSQL = "INSERT INTO Trips (TripID, CarID, date, departure, destination, num_seats_available) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(insertSQL);

            for (Trip trip : trips) {
                preparedStatement.setInt(1, trip.getTripID());
                preparedStatement.setInt(2, trip.getCarID());
                preparedStatement.setString(3, trip.getDate());
                preparedStatement.setString(4, trip.getDeparture());
                preparedStatement.setString(5, trip.getDestination());
                preparedStatement.setInt(6, trip.getNum_seats_available());

                rowsInserted += preparedStatement.executeUpdate();
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }

    @Override
    public int insertBookings(Booking[] bookings) {
        int rowsInserted = 0;
        String insertSQL = "INSERT INTO Bookings (TripID, PIN, booking_status) VALUES (?, ?, ?)";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(insertSQL);

            for (Booking booking : bookings) {
                preparedStatement.setInt(1, booking.getTripID());
                preparedStatement.setInt(2, booking.getPIN());
                preparedStatement.setString(3, booking.getBooking_status());

                rowsInserted += preparedStatement.executeUpdate();
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }


    // 3 Find all participants who are recorded as both drivers and passengers
    @Override
    public Participant[] getBothPassengersAndDrivers() {
        List<Participant> result = new ArrayList<>();

        String query =
                "SELECT DISTINCT p.PIN, p.p_name, p.age " +
                        "FROM Participants p " +
                        "INNER JOIN Drivers d ON p.PIN = d.PIN " +
                        "INNER JOIN Passengers ps ON p.PIN = ps.PIN " +
                        "ORDER BY p.PIN ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String pName = resultSet.getString("p_name");
                int age = resultSet.getInt("age");
                int pin = resultSet.getInt("PIN");

                Participant participant = new Participant(pin, pName, age);
                result.add(participant);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new Participant[0]);
    }


    // 4 Find the PINs, names, ages, and ratings of drivers who do not own any cars
    @Override
    public QueryResult.DriverPINNameAgeRating[] getDriversWithNoCars() {
        List<QueryResult.DriverPINNameAgeRating> result = new ArrayList<>();

        String query =
                "SELECT p.PIN, p.p_name, p.age, d.rating " +
                        "FROM Drivers d " +
                        "INNER JOIN Participants p ON d.PIN = p.PIN " +
                        "LEFT JOIN Cars c ON d.PIN = c.PIN " +
                        "WHERE c.PIN IS NULL " +
                        "ORDER BY p.PIN ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String pName = resultSet.getString("p_name");
                int age = resultSet.getInt("age");
                int pin = resultSet.getInt("PIN");
                double rating = resultSet.getDouble("rating");

                QueryResult.DriverPINNameAgeRating driver =
                        new QueryResult.DriverPINNameAgeRating(pin, pName, age, rating);
                result.add(driver);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new QueryResult.DriverPINNameAgeRating[0]);
    }


    // 5 Delete Drivers who do not own any cars
    @Override
    public int deleteDriversWithNoCars() {
        int rowsDeleted = 0;

        String deleteQuery =
                "DELETE FROM Drivers " +
                        "WHERE PIN IN (" +
                        "    SELECT d.PIN " +
                        "    FROM Drivers d " +
                        "    LEFT JOIN Cars c ON d.PIN = c.PIN " +
                        "    WHERE c.PIN IS NULL" +
                        ")";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(deleteQuery);
            rowsDeleted = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsDeleted;
    }


    // 6 Find all cars that are not taken part in any trips
    @Override
    public Car[] getCarsWithNoTrips() {
        List<Car> result = new ArrayList<>();

        String query =
                "SELECT c.CarID, c.PIN, c.color, c.brand " +
                        "FROM Cars c " +
                        "LEFT JOIN Trips t ON c.CarID = t.CarID " +
                        "WHERE t.CarID IS NULL " +
                        "ORDER BY c.CarID ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String color = resultSet.getString("color");
                String brand = resultSet.getString("brand");
                int carID = resultSet.getInt("CarID");
                int pin = resultSet.getInt("PIN");

                Car car = new Car(carID, pin, color, brand);
                result.add(car);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new Car[0]);
    }


    // 7 Find all passengers who didn't book any trips
    @Override
    public Passenger[] getPassengersWithNoBooks() {
        List<Passenger> result = new ArrayList<>();

        String query =
                "SELECT p.PIN, p.membership_status " +
                        "FROM Passengers p " +
                        "LEFT JOIN Bookings b ON p.PIN = b.PIN " +
                        "WHERE b.PIN IS NULL " +
                        "ORDER BY p.PIN ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int pin = resultSet.getInt("PIN");
                String membershipStatus = resultSet.getString("membership_status");

                Passenger passenger = new Passenger(pin, membershipStatus);
                result.add(passenger);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new Passenger[0]);
    }


    // 8 Find all trips that depart from the specified city to specified destination city on specific date
    @Override
    public Trip[] getTripsFromToCitiesOnSpecificDate(String departure, String destination, String date) {
        List<Trip> result = new ArrayList<>();

        String query =
                "SELECT TripID, CarID, date, departure, destination, num_seats_available " +
                        "FROM Trips " +
                        "WHERE departure = ? AND destination = ? AND date = ? " +
                        "ORDER BY TripID ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);

            preparedStatement.setString(1, departure);
            preparedStatement.setString(2, destination);
            preparedStatement.setString(3, date);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String tripDate = resultSet.getString("date");
                String tripDeparture = resultSet.getString("departure");
                String tripDestination = resultSet.getString("destination");
                int carID = resultSet.getInt("CarID");
                int tripID = resultSet.getInt("TripID");
                int numSeatsAvailable = resultSet.getInt("num_seats_available");

                Trip trip = new Trip(tripID, carID, tripDate, tripDeparture,
                        tripDestination, numSeatsAvailable);
                result.add(trip);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new Trip[0]);
    }


    // 9 Find the PINs, names, ages, and membership_status of passengers who have bookings on all trips destined at a particular city
    @Override
    public QueryResult.PassengerPINNameAgeMembershipStatus[] getPassengersWithBookingsToAllTripsForCity(String city) {
        List<QueryResult.PassengerPINNameAgeMembershipStatus> result = new ArrayList<>();

        String query =
                "SELECT DISTINCT pa.PIN, pa.p_name, pa.age, ps.membership_status " +
                        "FROM Participants pa " +
                        "JOIN Passengers ps ON pa.PIN = ps.PIN " +
                        "WHERE NOT EXISTS ( " +
                        "    SELECT t.TripID " +
                        "    FROM Trips t " +
                        "    WHERE t.destination = ? " +
                        "    AND NOT EXISTS ( " +
                        "        SELECT b.TripID " +
                        "        FROM Bookings b " +
                        "        WHERE b.TripID = t.TripID " +
                        "        AND b.PIN = pa.PIN " +
                        "    ) " +
                        ") " +
                        "AND EXISTS ( " +
                        "    SELECT 1 " +
                        "    FROM Trips t " +
                        "    WHERE t.destination = ? " +
                        ") " +
                        "ORDER BY pa.PIN ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);

            preparedStatement.setString(1, city);
            preparedStatement.setString(2, city);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                String pName = resultSet.getString("p_name");
                int pin = resultSet.getInt("PIN");
                int age = resultSet.getInt("age");
                String membershipStatus = resultSet.getString("membership_status");

                QueryResult.PassengerPINNameAgeMembershipStatus passenger =
                        new QueryResult.PassengerPINNameAgeMembershipStatus(pin, pName, age, membershipStatus);
                result.add(passenger);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new QueryResult.PassengerPINNameAgeMembershipStatus[0]);
    }


    // 10 For a given driver PIN, find the CarIDs that the driver owns and were booked at most twice.
    @Override
    public Integer[] getDriverCarsWithAtMost2Bookings(int driverPIN) {
        List<Integer> result = new ArrayList<>();

        String query =
                "SELECT c.CarID " +
                        "FROM Cars c " +
                        "LEFT JOIN Trips t ON c.CarID = t.CarID " +
                        "LEFT JOIN Bookings b ON t.TripID = b.TripID " +
                        "WHERE c.PIN = ? " +
                        "GROUP BY c.CarID " +
                        "HAVING COUNT(DISTINCT b.TripID) <= 2 " +
                        "ORDER BY c.CarID ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            preparedStatement.setInt(1, driverPIN);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int carID = resultSet.getInt("CarID");
                result.add(carID);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new Integer[0]);
    }


    // 11 Find the average age of passengers with "Confirmed" bookings (i.e., booking_status is ”Confirmed”) on trips departing from a given city and within a specified date range
    @Override
    public Double getAvgAgeOfPassengersDepartFromCityBetweenTwoDates(String city, String start_date, String end_date) {
        Double averageAge = null;

        String query =
                "SELECT AVG(CAST(p.age AS DOUBLE)) as avg_age " +
                        "FROM Participants p " +
                        "JOIN Bookings b ON p.PIN = b.PIN " +
                        "JOIN Trips t ON b.TripID = t.TripID " +
                        "WHERE t.departure = ? " +
                        "AND t.date >= ? " +
                        "AND t.date <= ? " +
                        "AND b.booking_status = 'Confirmed'";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);

            preparedStatement.setString(1, city);
            preparedStatement.setString(2, start_date);
            preparedStatement.setString(3, end_date);

            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                averageAge = resultSet.getDouble("avg_age");
                if (resultSet.wasNull()) {
                    averageAge = null;
                }
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return averageAge;
    }


    // 12 Find Passengers in a Given Trip.
    @Override
    public QueryResult.PassengerPINNameAgeMembershipStatus[] getPassengerInGivenTrip(int TripID) {
        List<QueryResult.PassengerPINNameAgeMembershipStatus> result = new ArrayList<>();

        String query =
                "SELECT pa.PIN, pa.p_name, pa.age, ps.membership_status " +
                        "FROM Participants pa " +
                        "JOIN Passengers ps ON pa.PIN = ps.PIN " +
                        "JOIN Bookings b ON ps.PIN = b.PIN " +
                        "WHERE b.TripID = ? " +
                        "ORDER BY pa.PIN ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            preparedStatement.setInt(1, TripID);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String pName = resultSet.getString("p_name");
                int pin = resultSet.getInt("PIN");
                int age = resultSet.getInt("age");
                String membershipStatus = resultSet.getString("membership_status");

                QueryResult.PassengerPINNameAgeMembershipStatus passenger =
                        new QueryResult.PassengerPINNameAgeMembershipStatus(pin, pName, age, membershipStatus);
                result.add(passenger);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new QueryResult.PassengerPINNameAgeMembershipStatus[0]);
    }


    // 13 Find Drivers’ Scores
    @Override
    public QueryResult.DriverScoreRatingNumberOfBookingsPIN[] getDriversScores() {
        List<QueryResult.DriverScoreRatingNumberOfBookingsPIN> result = new ArrayList<>();

        String query =
                "SELECT " +
                        "    d.PIN AS DriverPIN, " +
                        "    d.rating, " +
                        "    (" +
                        "        SELECT COUNT(*) " +
                        "        FROM Bookings b " +
                        "        JOIN Trips t ON b.TripID = t.TripID " +
                        "        JOIN Cars c ON t.CarID = c.CarID " +
                        "        WHERE c.PIN = d.PIN" +
                        "    ) AS numberOfBookings, " +
                        "    (" +
                        "        d.rating * " +
                        "        (" +
                        "            SELECT COUNT(*) " +
                        "            FROM Bookings b " +
                        "            JOIN Trips t ON b.TripID = t.TripID " +
                        "            JOIN Cars c ON t.CarID = c.CarID " +
                        "            WHERE c.PIN = d.PIN" +
                        "        )" +
                        "    ) AS driver_score " +
                        "FROM Drivers d " +
                        "WHERE EXISTS (" +
                        "    SELECT 1 " +
                        "    FROM Cars c2 " +
                        "    JOIN Trips t2 ON c2.CarID = t2.CarID " +
                        "    JOIN Bookings b2 ON t2.TripID = b2.TripID " +
                        "    WHERE c2.PIN = d.PIN" +
                        ") " +
                        "ORDER BY driver_score DESC, DriverPIN ASC";

        try (PreparedStatement stmt = this.connection.prepareStatement(query)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(buildDriverScoreObject(rs));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error executing driver scores query: " + e.getMessage());
            e.printStackTrace();
        }

        return result.toArray(new QueryResult.DriverScoreRatingNumberOfBookingsPIN[0]);
    }

    // Helper method to build driver score object from result set
    private QueryResult.DriverScoreRatingNumberOfBookingsPIN buildDriverScoreObject(ResultSet rs)
            throws SQLException {
        return new QueryResult.DriverScoreRatingNumberOfBookingsPIN(
                rs.getDouble("driver_score"),
                rs.getDouble("rating"),
                rs.getInt("numberOfBookings"),
                rs.getInt("DriverPIN")
        );
    }


    // 14 Find average ratings of drivers who have trips destined to each city
    @Override
    public QueryResult.CityAndAverageDriverRating[] getDriversAverageRatingsToEachDestinatedCity() {
        List<QueryResult.CityAndAverageDriverRating> result = new ArrayList<>();

        String query =
                "SELECT t.destination, AVG(CAST(d.rating AS DOUBLE)) as avg_rating " +
                        "FROM Trips t " +
                        "JOIN Cars c ON t.CarID = c.CarID " +
                        "JOIN Drivers d ON c.PIN = d.PIN " +
                        "GROUP BY t.destination " +
                        "ORDER BY t.destination ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                double avgRating = resultSet.getDouble("avg_rating");
                String city = resultSet.getString("destination");

                QueryResult.CityAndAverageDriverRating cityRating =
                        new QueryResult.CityAndAverageDriverRating(city, avgRating);
                result.add(cityRating);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new QueryResult.CityAndAverageDriverRating[0]);
    }


    // 15 Find total number of bookings of passengers for each membership status
    @Override
    public QueryResult.MembershipStatusAndTotalBookings[] getTotalBookingsEachMembershipStatus() {
        List<QueryResult.MembershipStatusAndTotalBookings> result = new ArrayList<>();

        String query =
                "SELECT p.membership_status, " +
                        "       COALESCE(booking_counts.total_bookings, 0) as total_bookings " +
                        "FROM (" +
                        "    SELECT DISTINCT membership_status " +
                        "    FROM Passengers" +
                        ") p " +
                        "LEFT JOIN (" +
                        "    SELECT ps.membership_status, COUNT(*) as total_bookings " +
                        "    FROM Passengers ps " +
                        "    JOIN Bookings b ON ps.PIN = b.PIN " +
                        "    GROUP BY ps.membership_status" +
                        ") booking_counts " +
                        "ON p.membership_status = booking_counts.membership_status " +
                        "ORDER BY p.membership_status ASC";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String membershipStatus = resultSet.getString("membership_status");
                int totalBookings = resultSet.getInt("total_bookings");

                QueryResult.MembershipStatusAndTotalBookings statusBookings =
                        new QueryResult.MembershipStatusAndTotalBookings(membershipStatus, totalBookings);
                result.add(statusBookings);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result.toArray(new QueryResult.MembershipStatusAndTotalBookings[0]);
    }


    // 16 For the drivers' ratings, if rating is smaller than 2.0 or equal to 2.0, update the rating by adding 0.5.
    @Override
    public int updateDriverRatings() {
        int rowsUpdated = 0;

        String updateQuery =
                "UPDATE Drivers " +
                        "SET rating = rating + 0.5 " +
                        "WHERE rating <= 2.0";

        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(updateQuery);
            rowsUpdated = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsUpdated;
    }


    @Override
    public Trip[] getTripsFromCity(String city) {

        /*****************************************************/
        /*****************************************************/
        /*****************  TODO    **************************/
        /*****************************************************/
        /*****************************************************/

        return new Trip[0];
    }


    @Override
    public Trip[] getTripsWithNoBooks() {

        /*****************************************************/
        /*****************************************************/
        /*****************  TODO *****************************/
        /*****************************************************/
        /*****************************************************/

        return new Trip[0];
    }


    @Override
    public QueryResult.DriverPINandTripIDandNumberOfBookings[] getTheMostBookedTripsPerDriver() {

        /*****************************************************/
        /*****************************************************/
        /*****************  TODO *****************************/
        /*****************************************************/
        /*****************************************************/

        return new QueryResult.DriverPINandTripIDandNumberOfBookings[0];
    }


    @Override
    public QueryResult.FullCars[] getFullCars() {

        /*****************************************************/
        /*****************************************************/
        /*****************  TODO *****************************/
        /*****************************************************/
        /*****************************************************/

        return new QueryResult.FullCars[0];
    }

}
