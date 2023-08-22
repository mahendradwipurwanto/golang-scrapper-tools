package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/joho/godotenv"
)

type FileUploadStatus struct {
	ID     uint   `json:"id"`
	URL    string `json:"url"`
	Status string `json:"status"`
}

type Files struct {
	ID       uint
	DriveURL string `gorm:"column:Foto"`
	Column   string `gorm:"column:nama_file"`
}

func main() {
	// Load environment variables from .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")

	tableName := os.Getenv("TABLE_NAME")
	urlColumnName := os.Getenv("URL_COLUMN_NAME")
	fileNameColumnName := os.Getenv("FILE_NAME_COLUMN_NAME")
	rawUrlColumnName := os.Getenv("RAW_URL_COLUMN_NAME")
	tableWhereColumn := os.Getenv("TABLE_WHERE_COLUMN")
	tableWhereValue := os.Getenv("TABLE_WHERE_VALUE")
	directoryBase := os.Getenv("DIRECTORY_BASE")
	fileNameHost := os.Getenv("FILE_NAME_HOST")
	fileNamePrefix := os.Getenv("FILE_NAME_PREFIX")

	dbConnectionStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", dbUser, dbPass, dbHost, dbPort, dbName)

	db, err := gorm.Open("mysql", dbConnectionStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	var data []Files

	// Fetch data based on the specific column condition
	query := fmt.Sprintf("SELECT id, %s, %s FROM %s WHERE %s = '%s' AND %s IS NULL", urlColumnName, fileNameColumnName, tableName, tableWhereColumn, tableWhereValue, fileNameColumnName)
	if err := db.Raw(query).Scan(&data).Error; err != nil {
		log.Fatal("Failed to retrieve data from database:", err)
	}

	totalData := len(data)

	var successfulUploads []FileUploadStatus
	var failedUploads []FileUploadStatus

	currentData := 1
	requestDelay := 5 * time.Second
	for _, file := range data {
		// Log the column value for debugging
		log.Printf("File %d/%d | File ID: %d, Drive URL: %s\n", currentData, totalData, file.ID, file.DriveURL)
		newURL, newFileName, saveSuccess := saveFileLocally(file.ID, file.DriveURL, directoryBase, file.Column, fileNamePrefix)

		if saveSuccess {
			successfulUploads = append(successfulUploads, FileUploadStatus{ID: file.ID, URL: newURL, Status: "success"})

			// Update the drive_url and new column with the new values
			db.Table(tableName).Where("id = ?", file.ID).Updates(map[string]interface{}{
				urlColumnName:      fmt.Sprintf("%s/%s", fileNameHost, newURL),
				fileNameColumnName: newFileName,
				rawUrlColumnName:   file.DriveURL,
			})
		} else {
			failedUploads = append(failedUploads, FileUploadStatus{ID: file.ID, URL: file.DriveURL, Status: "failed"})
		}
		currentData = currentData + 1
		time.Sleep(requestDelay)
	}

	logResults(successfulUploads, failedUploads)
}

func saveFileLocally(fileId uint, fileURL, directoryBase, subDirectory, fileNamePrefix string) (string, string, bool) {
	client := &http.Client{}

	req, err := http.NewRequest("GET", fileURL, nil)
	if err != nil {
		log.Println("Failed to create request:", err)
		return fileURL, "", false
	}

	// Set the User-Agent header
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.203")

	response, err := client.Do(req)
	if err != nil {
		log.Println("Failed to download "+fileURL+" file:", err)
		return fileURL, "", false
	}
	defer response.Body.Close()

	fileData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Println("Failed to read file data:", err)
		return "", "", false
	}

	// Extract the file extension from the URL
	fileExtension := filepath.Ext(fileURL)

	if fileExtension == "" {
		contentType := response.Header.Get("Content-Type")
		fileExtensions, err := mime.ExtensionsByType(contentType)
		if err != nil || len(fileExtensions) == 0 {
			log.Println("Failed to determine file extension from Content-Type:", err)
			fileExtensions = []string{".unknown"} // Default to ".unknown" extension
		}

		// Extract the extension (without the square brackets)
		ext := fileExtensions[0]
		fileExtension = ext
	}

	// cek if can't get extension
	if fileExtension == "" {
		log.Println(fmt.Sprintf("Failed to get file extension id: %v, url: %s", fileId, fileURL))
		return "", "", false
	}

	//check if fileExtension contains .htm
	if strings.Contains(fileExtension, ".htm") {
		log.Println(fmt.Sprintf("Failed to get file extension id: %v, cause %d", fileId, response.StatusCode))
		log.Println(response)
		panic("Failed to get file extension")
	}

	log.Println(fileExtension)

	fileIdStr := strconv.FormatUint(uint64(fileId), 10)
	localFileName := fmt.Sprintf("%s-%s%s", fileNamePrefix, fileIdStr, fileExtension) // Set extension here
	localFilePath := fmt.Sprintf("%s/%s/%s", directoryBase, subDirectory, localFileName)
	localFilePath = strings.ReplaceAll(localFilePath, "//", "/")
	// Create the subdirectories if they don't exist
	if err := os.MkdirAll(fmt.Sprintf("%s", directoryBase), os.ModePerm); err != nil {
		log.Println("Failed to create subdirectories:", err)
		return "", "", false
	}

	err = ioutil.WriteFile(localFilePath, fileData, 0644)
	if err != nil {
		log.Println("Failed to save file locally:", err)
		return "", "", false
	}

	log.Printf("File %s saved successfully locally\n", fileURL)
	return localFilePath, localFileName, true
}

func logResults(successfulUploads, failedUploads []FileUploadStatus) {
	results := struct {
		SuccessfulUploads []FileUploadStatus `json:"successful_uploads"`
		FailedUploads     []FileUploadStatus `json:"failed_uploads"`
	}{
		SuccessfulUploads: successfulUploads,
		FailedUploads:     failedUploads,
	}

	logFile, err := os.Create("upload_results.json")
	if err != nil {
		log.Println("Failed to create log file:", err)
		return
	}
	defer logFile.Close()

	encoder := json.NewEncoder(logFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		log.Println("Failed to encode log data:", err)
	}
}
