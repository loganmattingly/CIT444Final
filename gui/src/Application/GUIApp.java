package Application;

import javafx.application.Application;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.Stage;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class GUIApp extends Application {

    private static final String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";
    private static final String DB_USER = "system";
    private static final String DB_PASSWORD = "jm64108034";

    private ObservableList<Hotel> hotelData = FXCollections.observableArrayList();
    private ObservableList<String> cityData = FXCollections.observableArrayList();
    private ObservableList<String> countryData = FXCollections.observableArrayList();
    private Map<Integer, HotelRatings> ratingsData = new HashMap<>();

    // UI Components
    private Label loadingLabel;
    private ComboBox<String> cityComboBox;
    private ComboBox<String> countryComboBox;
    private TextField searchTextField;
    private ListView<String> hotelListView;
    private TableView<Hotel> hotelTableView;
    private Label hotelTitleLabel;
    private Label hotelRatingsLabel;
    
    // Advanced Search Components
    private Slider serviceSlider;
    private Slider priceSlider;
    private Slider roomSlider;
    private Slider locationSlider;
    private Slider overallSlider;
    private Button advancedSearchButton;
    private Button resetFiltersButton;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("CIT444 - Hotel Ratings Analysis System");
        primaryStage.setWidth(1000);
        primaryStage.setHeight(700);

        showLoadingScreen(primaryStage);
        loadDataInBackground(primaryStage);
    }

    private void showLoadingScreen(Stage primaryStage) {
        VBox loadingBox = new VBox(20);
        loadingBox.setAlignment(Pos.CENTER);
        loadingBox.setPadding(new Insets(50));
        
        loadingLabel = new Label("Loading Hotel Data...");
        loadingLabel.setFont(Font.font("Arial", FontWeight.BOLD, 18));
        
        ProgressIndicator progressIndicator = new ProgressIndicator();
        
        loadingBox.getChildren().addAll(loadingLabel, progressIndicator);
        
        Scene loadingScene = new Scene(loadingBox, 400, 200);
        primaryStage.setScene(loadingScene);
        primaryStage.show();
    }

    private void loadDataInBackground(Stage primaryStage) {
        Task<Void> loadDataTask = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                updateMessage("Loading hotel data...");
                loadHotelData();

                updateMessage("Loading city data...");
                loadCityData();

                updateMessage("Loading country data...");
                loadCountryData();

                updateMessage("Loading ratings data...");
                loadRatingsData();

                return null;
            }

            @Override
            protected void succeeded() {
                super.succeeded();
                initializeMainGUI(primaryStage);
            }

            @Override
            protected void failed() {
                super.failed();
                showErrorAlert("Data loading failed: " + getException().getMessage());
            }
            
            @Override
            protected void updateMessage(String message) {
                super.updateMessage(message);
                if (loadingLabel != null) {
                    loadingLabel.setText(message);
                }
            }
        };

        new Thread(loadDataTask).start();
    }

    private void loadHotelData() {
        String sql = "SELECT HOTELID, NAME, CITY, COUNTRY FROM HOTEL ORDER BY NAME";
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            int totalRows = getTotalRowCount("HOTEL");
            int currentRow = 0;

            while (rs.next()) {
                int hotelId = rs.getInt("HOTELID");
                String hotelName = rs.getString("NAME");
                String city = rs.getString("CITY");
                String country = rs.getString("COUNTRY");

                Hotel hotel = new Hotel(hotelId, hotelName, city, country);
                hotelData.add(hotel);

                currentRow++;
                if (currentRow % 100 == 0) {
                    System.out.println("Processed hotel " + currentRow + " of " + totalRows + 
                                     " (" + (currentRow * 100 / totalRows) + "% complete)");
                }
            }
            System.out.println("✓ Loaded " + hotelData.size() + " hotels");

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error loading hotel data: " + e.getMessage(), e);
        }
    }

    private void loadRatingsData() {
        String sql = "SELECT HOTELID, AVG_SERVICE, AVG_PRICE, AVG_ROOM, " +
                     "AVG_LOCATION, AVG_OVERALL, TOTAL_REVIEWS FROM RATINGSAVERAGE";
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            int totalRatings = getTotalRowCount("RATINGSAVERAGE");
            int currentRating = 0;

            System.out.println("Loading ratings data...");

            while (rs.next()) {
                int hotelId = rs.getInt("HOTELID");
                double serviceScore = rs.getDouble("AVG_SERVICE");
                double priceScore = rs.getDouble("AVG_PRICE");
                double roomScore = rs.getDouble("AVG_ROOM");
                double locationScore = rs.getDouble("AVG_LOCATION");
                double overallScore = rs.getDouble("AVG_OVERALL");
                int totalReviews = rs.getInt("TOTAL_REVIEWS");

                HotelRatings ratings = new HotelRatings(hotelId, serviceScore, priceScore, 
                                                       roomScore, locationScore, overallScore, totalReviews);
                ratingsData.put(hotelId, ratings);

                currentRating++;
                if (currentRating % 100 == 0) {
                    System.out.println("Processed rating " + currentRating + " of " + totalRatings);
                }
            }
            System.out.println("✓ Loaded " + ratingsData.size() + " hotel ratings");

        } catch (SQLException e) {
            System.err.println("Error loading ratings data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadCityData() {
        String sql = "SELECT DISTINCT CITY FROM HOTEL ORDER BY CITY";
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                cityData.add(rs.getString("CITY"));
            }
            System.out.println("✓ Loaded " + cityData.size() + " cities");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadCountryData() {
        String sql = "SELECT DISTINCT COUNTRY FROM HOTEL ORDER BY COUNTRY";
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                countryData.add(rs.getString("COUNTRY"));
            }
            System.out.println("✓ Loaded " + countryData.size() + " countries");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private int getTotalRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    private void initializeMainGUI(Stage primaryStage) {
        // Create main container
        BorderPane mainPane = new BorderPane();
        mainPane.setPadding(new Insets(15));

        // Create top section with filters
        VBox topSection = createTopSection();
        mainPane.setTop(topSection);

        // Create center section with hotel list and details
        HBox centerSection = createCenterSection();
        mainPane.setCenter(centerSection);

        // Create bottom section with advanced search
        VBox bottomSection = createAdvancedSearchSection();
        mainPane.setBottom(bottomSection);

        Scene mainScene = new Scene(mainPane);
        primaryStage.setScene(mainScene);
        primaryStage.show();
    }

    private VBox createTopSection() {
        VBox topSection = new VBox(15);
        topSection.setPadding(new Insets(15));
        topSection.setStyle("-fx-background-color: #f4f4f4; -fx-border-color: #ddd; -fx-border-width: 0 0 1 0;");

        // Title
        Label titleLabel = new Label("Hotel Ratings Analysis System");
        titleLabel.setFont(Font.font("Arial", FontWeight.BOLD, 24));
        titleLabel.setTextFill(Color.DARKBLUE);

        // Search and filter controls
        HBox filterBox = new HBox(15);
        filterBox.setAlignment(Pos.CENTER_LEFT);

        // Search field
        searchTextField = new TextField();
        searchTextField.setPromptText("Search hotels by name...");
        searchTextField.setPrefWidth(250);
        searchTextField.textProperty().addListener((observable, oldValue, newValue) -> 
            filterHotels(newValue, cityComboBox.getValue(), countryComboBox.getValue()));

        // City filter
        cityComboBox = new ComboBox<>(cityData);
        cityComboBox.setPromptText("All Cities");
        cityComboBox.setPrefWidth(150);
        cityComboBox.setOnAction(event -> 
            filterHotels(searchTextField.getText(), cityComboBox.getValue(), countryComboBox.getValue()));

        // Country filter
        countryComboBox = new ComboBox<>(countryData);
        countryComboBox.setPromptText("All Countries");
        countryComboBox.setPrefWidth(150);
        countryComboBox.setOnAction(event -> 
            filterHotels(searchTextField.getText(), cityComboBox.getValue(), countryComboBox.getValue()));

        // Reset button
        resetFiltersButton = new Button("Reset Filters");
        resetFiltersButton.setOnAction(event -> resetFilters());

        filterBox.getChildren().addAll(
            new Label("Search:"), searchTextField,
            new Label("City:"), cityComboBox,
            new Label("Country:"), countryComboBox,
            resetFiltersButton
        );

        topSection.getChildren().addAll(titleLabel, filterBox);
        return topSection;
    }

    private HBox createCenterSection() {
        HBox centerSection = new HBox(20);
        centerSection.setPadding(new Insets(15));

        // Left side - Hotel list
        VBox leftPanel = new VBox(10);
        leftPanel.setPrefWidth(400);

        Label listLabel = new Label("Hotels");
        listLabel.setFont(Font.font("Arial", FontWeight.BOLD, 16));

        hotelListView = new ListView<>();
        ObservableList<String> hotelNames = FXCollections.observableArrayList();
        for (Hotel hotel : hotelData) {
            hotelNames.add(hotel.getName() + " - " + hotel.getCity());
        }
        hotelListView.setItems(hotelNames);
        hotelListView.setPrefHeight(400);

        hotelListView.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue != null) {
                displayHotelRatings(newValue);
            }
        });

        leftPanel.getChildren().addAll(listLabel, hotelListView);

        // Right side - Hotel details
        VBox rightPanel = new VBox(15);
        rightPanel.setPrefWidth(500);

        hotelTitleLabel = new Label("Select a hotel to view ratings");
        hotelTitleLabel.setFont(Font.font("Arial", FontWeight.BOLD, 18));
        hotelTitleLabel.setTextFill(Color.DARKBLUE);

        hotelRatingsLabel = new Label("");
        hotelRatingsLabel.setFont(Font.font("Arial", 14));
        hotelRatingsLabel.setWrapText(true);

        // Create ratings display with better formatting
        VBox ratingsBox = new VBox(10);
        ratingsBox.setPadding(new Insets(15));
        ratingsBox.setStyle("-fx-background-color: #f9f9f9; -fx-border-color: #ddd; -fx-border-radius: 5;");

        rightPanel.getChildren().addAll(hotelTitleLabel, ratingsBox, hotelRatingsLabel);

        centerSection.getChildren().addAll(leftPanel, rightPanel);
        return centerSection;
    }

    private VBox createAdvancedSearchSection() {
        VBox advancedSection = new VBox(10);
        advancedSection.setPadding(new Insets(15));
        advancedSection.setStyle("-fx-background-color: #e8f4f8; -fx-border-color: #b3d9f2; -fx-border-width: 1 0 0 0;");

        Label advancedLabel = new Label("Advanced Search - Filter by Minimum Ratings");
        advancedLabel.setFont(Font.font("Arial", FontWeight.BOLD, 16));

        // Create sliders for each category
        HBox slidersBox = new HBox(20);
        slidersBox.setAlignment(Pos.CENTER);

        serviceSlider = createRatingSlider("Service");
        priceSlider = createRatingSlider("Price");
        roomSlider = createRatingSlider("Room");
        locationSlider = createRatingSlider("Location");
        overallSlider = createRatingSlider("Overall");

        slidersBox.getChildren().addAll(serviceSlider, priceSlider, roomSlider, locationSlider, overallSlider);

        // Search button
        advancedSearchButton = new Button("Apply Advanced Filters");
        advancedSearchButton.setStyle("-fx-background-color: #4CAF50; -fx-text-fill: white; -fx-font-weight: bold;");
        advancedSearchButton.setOnAction(event -> performAdvancedSearch());

        advancedSection.getChildren().addAll(advancedLabel, slidersBox, advancedSearchButton);
        return advancedSection;
    }

    private Slider createRatingSlider(String category) {
        VBox sliderBox = new VBox(5);
        
        Label label = new Label(category);
        label.setFont(Font.font("Arial", FontWeight.BOLD, 12));
        
        Slider slider = new Slider(1, 5, 1);
        slider.setShowTickLabels(true);
        slider.setShowTickMarks(true);
        slider.setMajorTickUnit(1);
        slider.setBlockIncrement(0.5);
        slider.setSnapToTicks(true);
        slider.setPrefWidth(120);
        
        Label valueLabel = new Label("1.0");
        valueLabel.setFont(Font.font("Arial", 11));
        
        slider.valueProperty().addListener((obs, oldVal, newVal) -> {
            valueLabel.setText(String.format("%.1f", newVal.doubleValue()));
        });
        
        sliderBox.getChildren().addAll(label, slider, valueLabel);
        sliderBox.setAlignment(Pos.CENTER);
        
        return slider;
    }

    private void filterHotels(String searchTerm, String selectedCity, String selectedCountry) {
        ObservableList<String> filteredHotels = FXCollections.observableArrayList();
        
        for (Hotel hotel : hotelData) {
            boolean matchesSearch = searchTerm == null || searchTerm.isEmpty() || 
                                  hotel.getName().toLowerCase().contains(searchTerm.toLowerCase());
            boolean matchesCity = selectedCity == null || selectedCity.isEmpty() || 
                                 hotel.getCity().equals(selectedCity);
            boolean matchesCountry = selectedCountry == null || selectedCountry.isEmpty() || 
                                    hotel.getCountry().equals(selectedCountry);

            if (matchesSearch && matchesCity && matchesCountry) {
                filteredHotels.add(hotel.getName() + " - " + hotel.getCity());
            }
        }
        
        hotelListView.setItems(filteredHotels);
        
        // Update count label
        if (hotelTitleLabel != null) {
            hotelTitleLabel.setText("Found " + filteredHotels.size() + " hotels");
        }
    }

    private void performAdvancedSearch() {
        double minService = serviceSlider.getValue();
        double minPrice = priceSlider.getValue();
        double minRoom = roomSlider.getValue();
        double minLocation = locationSlider.getValue();
        double minOverall = overallSlider.getValue();
        
        ObservableList<String> filteredHotels = FXCollections.observableArrayList();
        int matchCount = 0;
        
        for (Hotel hotel : hotelData) {
            HotelRatings ratings = ratingsData.get(hotel.getHotelId());
            if (ratings != null) {
                boolean meetsCriteria = 
                    ratings.getServiceScore() >= minService &&
                    ratings.getPriceScore() >= minPrice &&
                    ratings.getRoomScore() >= minRoom &&
                    ratings.getLocationScore() >= minLocation &&
                    ratings.getOverallScore() >= minOverall;
                
                if (meetsCriteria) {
                    filteredHotels.add(hotel.getName() + " - " + hotel.getCity() + 
                                     " (Overall: " + String.format("%.1f", ratings.getOverallScore()) + ")");
                    matchCount++;
                }
            }
        }
        
        hotelListView.setItems(filteredHotels);
        hotelTitleLabel.setText("Advanced Search: " + matchCount + " hotels meet criteria");
        
        // Show search criteria
        hotelRatingsLabel.setText(String.format(
            "Search Criteria:\n" +
            "• Service ≥ %.1f\n• Price ≥ %.1f\n• Room ≥ %.1f\n• Location ≥ %.1f\n• Overall ≥ %.1f",
            minService, minPrice, minRoom, minLocation, minOverall
        ));
    }

    private void resetFilters() {
        searchTextField.clear();
        cityComboBox.getSelectionModel().clearSelection();
        countryComboBox.getSelectionModel().clearSelection();
        
        serviceSlider.setValue(1);
        priceSlider.setValue(1);
        roomSlider.setValue(1);
        locationSlider.setValue(1);
        overallSlider.setValue(1);
        
        filterHotels("", null, null);
        hotelTitleLabel.setText("Select a hotel to view ratings");
        hotelRatingsLabel.setText("");
    }

    private void displayHotelRatings(String hotelSelection) {
        // Extract hotel name from the list display format
        String hotelName = hotelSelection.split(" - ")[0];
        
        Hotel selectedHotel = null;
        for (Hotel hotel : hotelData) {
            if (hotel.getName().equals(hotelName)) {
                selectedHotel = hotel;
                break;
            }
        }

        if (selectedHotel != null) {
            HotelRatings ratings = ratingsData.get(selectedHotel.getHotelId());
            if (ratings != null) {
                hotelTitleLabel.setText(selectedHotel.getName());
                
                String ratingsText = String.format(
                    "📍 %s, %s\n\n" +
                    "⭐ Overall Rating: %.1f/5.0\n" +
                    "📊 Based on %d reviews\n\n" +
                    "Detailed Scores:\n" +
                    "• 🛎️  Service: %.1f/5.0\n" +
                    "• 💰 Price: %.1f/5.0\n" +
                    "• 🛏️  Room: %.1f/5.0\n" +
                    "• 📍 Location: %.1f/5.0",
                    selectedHotel.getCity(), selectedHotel.getCountry(),
                    ratings.getOverallScore(),
                    ratings.getTotalReviews(),
                    ratings.getServiceScore(),
                    ratings.getPriceScore(),
                    ratings.getRoomScore(),
                    ratings.getLocationScore()
                );
                
                hotelRatingsLabel.setText(ratingsText);
                hotelRatingsLabel.setStyle("-fx-font-size: 14px; -fx-line-spacing: 5px;");
            } else {
                hotelTitleLabel.setText(selectedHotel.getName());
                hotelRatingsLabel.setText("No ratings available for this hotel.\n\n" +
                                        "This hotel exists in our database but doesn't have\n" +
                                        "any analyzed reviews yet.");
                hotelRatingsLabel.setStyle("-fx-text-fill: #666; -fx-font-style: italic;");
            }
        }
    }

    private void showErrorAlert(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText("Database Error");
        alert.setContentText(message);
        alert.showAndWait();
    }

    // Data Model Classes
    public static class Hotel {
        private final int hotelId;
        private final String name;
        private final String city;
        private final String country;

        public Hotel(int hotelId, String name, String city, String country) {
            this.hotelId = hotelId;
            this.name = name;
            this.city = city;
            this.country = country;
        }

        public int getHotelId() { return hotelId; }
        public String getName() { return name; }
        public String getCity() { return city; }
        public String getCountry() { return country; }
    }

    public static class HotelRatings {
        private final int hotelId;
        private final double serviceScore;
        private final double priceScore;
        private final double roomScore;
        private final double locationScore;
        private final double overallScore;
        private final int totalReviews;

        public HotelRatings(int hotelId, double serviceScore, double priceScore, 
                           double roomScore, double locationScore, double overallScore, int totalReviews) {
            this.hotelId = hotelId;
            this.serviceScore = serviceScore;
            this.priceScore = priceScore;
            this.roomScore = roomScore;
            this.locationScore = locationScore;
            this.overallScore = overallScore;
            this.totalReviews = totalReviews;
        }

        public int getHotelId() { return hotelId; }
        public double getServiceScore() { return serviceScore; }
        public double getPriceScore() { return priceScore; }
        public double getRoomScore() { return roomScore; }
        public double getLocationScore() { return locationScore; }
        public double getOverallScore() { return overallScore; }
        public int getTotalReviews() { return totalReviews; }
    }
}