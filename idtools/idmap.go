/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
   Copyright The containerd Authors.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// Copied from https://github.com/containerd/containerd/blob/2ca3ff87255a4aa6b4244cb942033d45b6d44546/internal/userns/idmap.go

/*
   This file is copied and customized based on
   https://github.com/moby/moby/blob/master/pkg/idtools/idtools.go
*/

package idtools

import (
	"errors"
	"fmt"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
)

const invalidID = 1<<32 - 1

var invalidUser = User{Uid: invalidID, Gid: invalidID}

// User is a Uid and Gid pair of a user
//
//nolint:revive
type User struct {
	Uid uint32
	Gid uint32
}

// IDMap contains the mappings of Uids and Gids.
//
//nolint:revive
type IDMap struct {
	UidMap []specs.LinuxIDMapping `json:"UidMap"`
	GidMap []specs.LinuxIDMapping `json:"GidMap"`
}

// ToHost returns the host user ID pair for the container ID pair.
func (i IDMap) ToHost(pair User) (User, error) {
	var (
		target User
		err    error
	)

	if i.Empty() {
		return pair, nil
	}

	target.Uid, err = toHost(pair.Uid, i.UidMap)
	if err != nil {
		return invalidUser, err
	}
	target.Gid, err = toHost(pair.Gid, i.GidMap)
	if err != nil {
		return invalidUser, err
	}
	return target, nil
}

// Empty returns true if there are no id mappings
func (i IDMap) Empty() bool {
	return len(i.UidMap) == 0 && len(i.GidMap) == 0
}

// toHost takes an id mapping and a remapped ID, and translates the
// ID to the mapped host ID. If no map is provided, then the translation
// assumes a 1-to-1 mapping and returns the passed in id #
func toHost(contID uint32, idMap []specs.LinuxIDMapping) (uint32, error) {
	if idMap == nil {
		return contID, nil
	}
	for _, m := range idMap {
		high, err := safeSum(m.ContainerID, m.Size)
		if err != nil {
			break
		}
		if contID >= m.ContainerID && contID < high {
			hostID, err := safeSum(m.HostID, contID-m.ContainerID)
			if err != nil || hostID == invalidID {
				break
			}
			return hostID, nil
		}
	}
	return invalidID, fmt.Errorf("container ID %d cannot be mapped to a host ID", contID)
}

// Unmarshal deserialize the passed uidmap and gidmap strings
// into a IDMap object. Error is returned in case of failure
func (i *IDMap) Unmarshal(uidMap, gidMap string) error {
	unmarshal := func(str string, fn func(m specs.LinuxIDMapping)) error {
		if len(str) == 0 {
			return nil
		}
		for _, mapping := range strings.Split(str, ",") {
			m, err := deserializeLinuxIDMapping(mapping)
			if err != nil {
				return err
			}
			fn(m)
		}
		return nil
	}
	if err := unmarshal(uidMap, func(m specs.LinuxIDMapping) {
		i.UidMap = append(i.UidMap, m)
	}); err != nil {
		return err
	}
	return unmarshal(gidMap, func(m specs.LinuxIDMapping) {
		i.GidMap = append(i.GidMap, m)
	})
}

// deserializeLinuxIDMapping unmarshals a string to a LinuxIDMapping object
func deserializeLinuxIDMapping(str string) (specs.LinuxIDMapping, error) {
	var (
		hostID, ctrID, length int64
	)
	_, err := fmt.Sscanf(str, "%d:%d:%d", &ctrID, &hostID, &length)
	if err != nil {
		return specs.LinuxIDMapping{}, fmt.Errorf("input value %s unparsable: %w", str, err)
	}
	if ctrID < 0 || ctrID >= invalidID || hostID < 0 || hostID >= invalidID || length < 0 || length >= invalidID {
		return specs.LinuxIDMapping{}, fmt.Errorf("invalid mapping \"%s\"", str)
	}
	return specs.LinuxIDMapping{
		ContainerID: uint32(ctrID),
		HostID:      uint32(hostID),
		Size:        uint32(length),
	}, nil
}

// safeSum returns the sum of x and y. or an error if the result overflows
func safeSum(x, y uint32) (uint32, error) {
	z := x + y
	if z < x || z < y {
		return invalidID, errors.New("ID overflow")
	}
	return z, nil
}
