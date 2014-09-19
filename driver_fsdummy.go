package fsabstract

import (
	"io/ioutil"
	"os"
	"time"
)

// FSDummy is a simple filesystem driver, set by a basepath, in which all
// files are stored in a single directory. It doesn't scale, and should only
// be used for testing or limited applications.
type FSDummy struct {
	BasePath string `fsdconfig:"fsdummy.basepath"`
}

func (self *FSDummy) DriverName() string {
	return "fsdummy"
}

func (self *FSDummy) Configure(c map[string]string) {
	if v, exists := c["fsdummy.basePath"]; exists {
		self.BasePath = v
	}
}

func (self *FSDummy) Get(d FileStoreDescriptor) ([]byte, FileStoreLocation, error) {
	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return nil, FileStoreLocation{}, err
	}

	// Retrieve actual file data from disk
	fullPath := self.BasePath + string(os.PathSeparator) + l.Location
	c, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return nil, l, err
	}

	// Send everything back
	return c, l, nil
}

func (self *FSDummy) Put(d FileStoreDescriptor, c []byte) error {
	// Create new location
	l := FileStoreLocation{
		Id:       "", // dummy driver doesn't have a store name/id
		Driver:   self.DriverName(),
		Created:  time.Now(),
		Location: "file_" + string(d.Id), // not very creative
	}

	// Push out to filesystem
	fullPath := self.BasePath + string(os.PathSeparator) + l.Location
	err := ioutil.WriteFile(fullPath, c, 0777)
	if err != nil {
		return err
	}

	// Append location
	d.Location = append(d.Location, l)

	// No errors, send back
	return nil
}

func (self *FSDummy) Delete(d FileStoreDescriptor, l FileStoreLocation) error {
	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return err
	}

	// Delete from disk
	fullPath := self.BasePath + string(os.PathSeparator) + l.Location
	err = os.Remove(fullPath)
	if err != nil {
		return err
	}

	// Remove from mapping
	RemoveLocation(d, l)

	// No errors, send back
	return nil
}
