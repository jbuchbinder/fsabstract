package fsabstract

import (
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func init() {
	FileStoreDriverMap["dummy"] = func() FileStoreDriver {
		return new(FSDummy)
	}
}

// FSDummy is a simple filesystem driver, set by a basepath, in which all
// files are stored in a single directory. It doesn't scale, and should only
// be used for testing or limited applications.
type FSDummy struct {
	BasePath string `fsdconfig:"fs.dummy.basepath"`
}

func (self *FSDummy) DriverName() string {
	return "dummy"
}

func (self *FSDummy) Configure(c map[string]string) {
	if v, exists := c["fs.dummy.basepath"]; exists {
		self.BasePath = v
	}
}

func (self *FSDummy) Initialize() error {
	return os.MkdirAll(self.BasePath, 0700)
}

func (self *FSDummy) Get(d FileStoreDescriptor) ([]byte, FileStoreLocation, error) {
	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return nil, FileStoreLocation{}, err
	}

	// Retrieve actual file data from disk
	fullPath := /* self.BasePath + string(os.PathSeparator) + */ l.Location
	c, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return nil, l, err
	}

	// Send everything back
	return c, l, nil
}

func (self *FSDummy) Put(d FileStoreDescriptor, c []byte) (FileStoreDescriptor, error) {
	// Updated copy of descriptor
	dU := d

	// Create new location
	fullPath := self.BasePath + string(os.PathSeparator) + "file_" + strconv.FormatInt(dU.Id, 16) // hex
	l := FileStoreLocation{
		Id:       "", // dummy driver doesn't have a store name/id
		Driver:   self.DriverName(),
		Created:  time.Now(),
		Location: fullPath,
	}

	// Push out to filesystem
	err := ioutil.WriteFile(l.Location, c, 0777)
	if err != nil {
		return dU, err
	}

	// Append location
	if dU.Location == nil {
		dU.Location = make([]FileStoreLocation, 0)
	}
	dU.Location = append(d.Location, l)

	// No errors, send back
	return dU, nil
}

func (self *FSDummy) Delete(d FileStoreDescriptor, l FileStoreLocation) (FileStoreDescriptor, error) {
	dU := d

	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return dU, err
	}

	// Delete from disk
	err = os.Remove(l.Location)
	if err != nil {
		return dU, err
	}

	// Remove from mapping
	RemoveLocation(dU, l)

	// No errors, send back
	return dU, nil
}
