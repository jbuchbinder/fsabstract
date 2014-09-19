package fsabstract

import (
	"errors"
)

// LocationForDriver returns the first FileStoreLocation which is represented
// by the provided FileStoreDescriptor for the specified driver.
func LocationForDriver(desc FileStoreDescriptor, driver string) (FileStoreLocation, error) {
	if desc.Location == nil {
		return FileStoreLocation{}, errors.New("No locations : " + desc.ToString())
	}
	for _, v := range desc.Location {
		if v.Driver == driver {
			return v, nil
		}
	}
	return FileStoreLocation{}, errors.New("Driver " + driver + " not found : " + desc.ToString())
}

// RemoveLocation removes a FileStoreLocation object from the list stored in
// a FileStoreDescriptor object. This is a convenience method, necessary
// because go doesn't have a built-in delete functionality for arrays, just
// maps.
func RemoveLocation(d FileStoreDescriptor, l FileStoreLocation) {
	nl := make([]FileStoreLocation, 0)
	for _, v := range d.Location {
		if v != l {
			nl = append(nl, v)
		}
	}
	d.Location = nl
}
