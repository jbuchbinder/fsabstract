package fsabstract

import (
	"errors"
)

func Migrate(f FileStoreDescriptor, locFrom, locTo FileStoreLocation) (FileStoreDescriptor, error) {
	// Migrate file described by f from one location to another
	fU := f

	// Retrieve drivers
	dFrom := GetDriver(locFrom.Driver)
	if dFrom == nil {
		return fU, errors.New("Could not instantiate driver " + locFrom.Driver)
	}
	dTo := GetDriver(locTo.Driver)
	if dTo == nil {
		return fU, errors.New("Could not instantiate driver " + locTo.Driver)
	}

	// Get file data
	content, originalLocation, err := dFrom.Get(fU)
	if err != nil {
		return fU, err
	}

	// Put to destination driver
	fU, err = dTo.Put(fU, content)
	if err != nil {
		return fU, err
	}

	// Remove from source
	fU, err = dFrom.Delete(fU, originalLocation)
	if err != nil {
		return fU, err
	}

	return fU, nil
}
