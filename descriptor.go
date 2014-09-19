package fsabstract

import (
	"encoding/json"
	"time"
)

type FileStoreDescriptor struct {
	// Id is the database-type identifier for this file resource. It should
	// be used to uniquely identify the file.
	Id int64 `json:"id"`
	// Name is the standard filename for the file resource
	Name string `json:"filename"`
	// Type is the mimetype. This is completely arbitrary.
	Type string `json:"filetype"`
	// Size represents the size of the file resource in bytes
	Size int64 `json:"size"`
	// Created represents the time that this FileStoreDescriptor resource
	// was initially created/stored.
	Created time.Time `json:"created"`
	// Metadata describes arbitrary metadata. This is undefined by the
	// specification, and can be used by applications to store additional
	// key/value pairs in file storage.
	Metadata map[string]string `json:"metadata"`
	// Location is an array of FileStoreLocation objects. Each file can
	// be stored by more than one driver/location, which is required by
	// any sort of migration routine.
	Location []FileStoreLocation `json:"location"`
}

type FileStoreLocation struct {
	// Id represents the identification of the store in which this
	// driver has stored the instance of the file. For example, for an
	// S3 located file, this would represent the S3 bucket name.
	Id string `json:"storeId"`
	// Driver should correspond to driver.DriverName() so that the
	// corresponding driver can be located to load or manipulate this
	// instance of the file data.
	Driver string `json:"storeDriver"`
	// Location is a driver-specific description of the location of the
	// instance of the file data.
	Location string `json:"storeLocation"`
	// Created represents the time this FileStoreLocation object was
	// created/stored by the driver.
	Created time.Time `json:"storeCreated"`
}

// ToString handles JSON serialization transparently.
func (self *FileStoreDescriptor) ToString() string {
	b, err := json.Marshal(self)
	if err != nil {
		return ""
	}
	return string(b)
}
