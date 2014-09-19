package fsabstract

import (
	"errors"
)

func Migrate(f FileStoreDescriptor, locFrom, locTo FileStoreLocation) error {
	// Migrate file described by f from one location to another

	// Retrieve drivers
	dFrom := GetDriver(locFrom.Driver)
	if dFrom == nil {
		return errors.New("Could not instantiate driver " + locFrom.Driver)
	}
	dTo := GetDriver(locTo.Driver)
	if dTo == nil {
		return errors.New("Could not instantiate driver " + locTo.Driver)
	}

	// Get file data
	content, originalLocation, err := dFrom.Get(f)
	if err != nil {
		return err
	}

	// Put to destination driver
	err = dTo.Put(f, content)
	if err != nil {
		return err
	}

	// Remove from source
	err = dFrom.Delete(f, originalLocation)
	if err != nil {
		return err
	}

	return nil
}
