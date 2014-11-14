package fsabstract

import (
	"errors"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"strconv"
	"time"
)

func init() {
	FileStoreDriverMap["s3"] = func() FileStoreDriver {
		return new(FSS3)
	}
}

// FSS3 is an AWS S3 driver.
type FSS3 struct {
	BucketName string     `fsdconfig:"fs.s3.bucket"`
	AccessKey  string     `fsdconfig:"fs.s3.accesskey"`
	SecretKey  string     `fsdconfig:"fs.s3.secretkey"`
	RegionObj  aws.Region `fsdconfig:"fs.s3.region"`

	auth   *aws.Auth
	conn   *s3.S3
	bucket *s3.Bucket
}

func (self *FSS3) DriverName() string {
	return "s3"
}

func (self *FSS3) Configure(c map[string]string) {
	if v, exists := c["fs.s3.bucket"]; exists {
		self.BucketName = v
	}
	if v, exists := c["fs.s3.accesskey"]; exists {
		self.AccessKey = v
	}
	if v, exists := c["fs.s3.secretkey"]; exists {
		self.SecretKey = v
	}
	if v, exists := c["fs.s3.region"]; exists {
		if _, exists = aws.Regions[v]; exists {
			self.RegionObj = aws.Regions[v]
		} else {
			panic("Unable to resolve S3 region " + v)
		}
	}
}

func (self *FSS3) Initialize() error {
	auth, err := aws.GetAuth(self.AccessKey, self.SecretKey)
	if err != nil {
		return err
	}
	self.auth = &auth
	conn := s3.New(*self.auth, self.RegionObj)
	self.conn = conn
	if self.conn == nil {
		return errors.New("Unable to initialize s3 driver")
	}
	bucket := self.conn.Bucket(self.BucketName)
	self.bucket = bucket
	return nil
}

func (self *FSS3) Get(d FileStoreDescriptor) ([]byte, FileStoreLocation, error) {
	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return nil, FileStoreLocation{}, err
	}

	// Retrieve actual file data from disk
	c, err := self.bucket.Get(l.Location)
	if err != nil {
		return nil, l, err
	}

	// Send everything back
	return c, l, nil
}

func (self *FSS3) Put(d FileStoreDescriptor, c []byte) (FileStoreDescriptor, error) {
	dU := d

	// Create new location
	k := "fs_" + strconv.FormatInt(dU.Id, 16) + "_" + dU.Name
	l := FileStoreLocation{
		Id:       "", // dummy driver doesn't have a store name/id
		Driver:   self.DriverName(),
		Created:  time.Now(),
		Location: k,
	}

	// Push out to filesystem
	err := self.bucket.Put(
		k,
		c,
		d.Type,
		s3.BucketOwnerFull)
	if err != nil {
		return dU, err
	}

	// Append location
	if dU.Location == nil {
		dU.Location = make([]FileStoreLocation, 0)
	}
	dU.Location = append(dU.Location, l)

	// No errors, send back
	return dU, nil
}

func (self *FSS3) Delete(d FileStoreDescriptor, l FileStoreLocation) (FileStoreDescriptor, error) {
	dU := d

	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(dU, self.DriverName())
	if err != nil {
		return dU, err
	}

	// Delete from disk
	err = self.bucket.Del(l.Location)
	if err != nil {
		return dU, err
	}

	// Remove from mapping
	RemoveLocation(dU, l)

	// No errors, send back
	return dU, nil
}
