package util

type KafkaVersion struct {
	// 它是一个结构，而不仅仅是直接键入数组以使其不透明并阻止人员
	// 生成自己的任意版本
	version [4]uint
}

// NewKafkaVersion 主要、次要、非常次要、补丁
func NewKafkaVersion(major, minor, veryMinor, patch uint) KafkaVersion {
	return KafkaVersion{
		version: [4]uint{major, minor, veryMinor, patch},
	}
}

// IsAtLeast return true if and only if the version it is called on is
// greater than or equal to the version passed in:
//
//	V1.IsAtLeast(V2) // false
//	V2.IsAtLeast(V1) // true
func (v KafkaVersion) IsAtLeast(other KafkaVersion) bool {
	for i := range v.version {
		if v.version[i] > other.version[i] {
			return true
		} else if v.version[i] < other.version[i] {
			return false
		}
	}
	return true
}

// Effective constants defining the supported kafka versions.
var (
	V0_8_2_0  = NewKafkaVersion(0, 8, 2, 0)
	V0_8_2_1  = NewKafkaVersion(0, 8, 2, 1)
	V0_8_2_2  = NewKafkaVersion(0, 8, 2, 2)
	V0_9_0_0  = NewKafkaVersion(0, 9, 0, 0)
	V0_9_0_1  = NewKafkaVersion(0, 9, 0, 1)
	V0_10_0_0 = NewKafkaVersion(0, 10, 0, 0)
	V0_10_0_1 = NewKafkaVersion(0, 10, 0, 1)
	V0_10_1_0 = NewKafkaVersion(0, 10, 1, 0)
	V0_10_1_1 = NewKafkaVersion(0, 10, 1, 1)
	V0_10_2_0 = NewKafkaVersion(0, 10, 2, 0)
	V0_10_2_1 = NewKafkaVersion(0, 10, 2, 1)
	V0_10_2_2 = NewKafkaVersion(0, 10, 2, 2)
	V0_11_0_0 = NewKafkaVersion(0, 11, 0, 0)
	V0_11_0_1 = NewKafkaVersion(0, 11, 0, 1)
	V0_11_0_2 = NewKafkaVersion(0, 11, 0, 2)
	V1_0_0_0  = NewKafkaVersion(1, 0, 0, 0)
	V1_0_1_0  = NewKafkaVersion(1, 0, 1, 0)
	V1_0_2_0  = NewKafkaVersion(1, 0, 2, 0)
	V1_1_0_0  = NewKafkaVersion(1, 1, 0, 0)
	V1_1_1_0  = NewKafkaVersion(1, 1, 1, 0)
	V2_0_0_0  = NewKafkaVersion(2, 0, 0, 0)
	V2_0_1_0  = NewKafkaVersion(2, 0, 1, 0)
	V2_1_0_0  = NewKafkaVersion(2, 1, 0, 0)
	V2_1_1_0  = NewKafkaVersion(2, 1, 1, 0)
	V2_2_0_0  = NewKafkaVersion(2, 2, 0, 0)
	V2_2_1_0  = NewKafkaVersion(2, 2, 1, 0)
	V2_2_2_0  = NewKafkaVersion(2, 2, 2, 0)
	V2_3_0_0  = NewKafkaVersion(2, 3, 0, 0)
	V2_3_1_0  = NewKafkaVersion(2, 3, 1, 0)
	V2_4_0_0  = NewKafkaVersion(2, 4, 0, 0)
	V2_4_1_0  = NewKafkaVersion(2, 4, 1, 0)
	V2_5_0_0  = NewKafkaVersion(2, 5, 0, 0)
	V2_5_1_0  = NewKafkaVersion(2, 5, 1, 0)
	V2_6_0_0  = NewKafkaVersion(2, 6, 0, 0)
	V2_6_1_0  = NewKafkaVersion(2, 6, 1, 0)
	V2_6_2_0  = NewKafkaVersion(2, 6, 2, 0)
	V2_6_3_0  = NewKafkaVersion(2, 6, 3, 0)
	V2_7_0_0  = NewKafkaVersion(2, 7, 0, 0)
	V2_7_1_0  = NewKafkaVersion(2, 7, 1, 0)
	V2_7_2_0  = NewKafkaVersion(2, 7, 2, 0)
	V2_8_0_0  = NewKafkaVersion(2, 8, 0, 0)
	V2_8_1_0  = NewKafkaVersion(2, 8, 1, 0)
	V2_8_2_0  = NewKafkaVersion(2, 8, 2, 0)
	V3_0_0_0  = NewKafkaVersion(3, 0, 0, 0)
	V3_0_1_0  = NewKafkaVersion(3, 0, 1, 0)
	V3_0_2_0  = NewKafkaVersion(3, 0, 2, 0)
	V3_1_0_0  = NewKafkaVersion(3, 1, 0, 0)
	V3_1_1_0  = NewKafkaVersion(3, 1, 1, 0)
	V3_1_2_0  = NewKafkaVersion(3, 1, 2, 0)
	V3_2_0_0  = NewKafkaVersion(3, 2, 0, 0)
	V3_2_1_0  = NewKafkaVersion(3, 2, 1, 0)
	V3_2_2_0  = NewKafkaVersion(3, 2, 2, 0)
	V3_2_3_0  = NewKafkaVersion(3, 2, 3, 0)
	V3_3_0_0  = NewKafkaVersion(3, 3, 0, 0)
	V3_3_1_0  = NewKafkaVersion(3, 3, 1, 0)
	V3_3_2_0  = NewKafkaVersion(3, 3, 2, 0)

	SupportedVersions = []KafkaVersion{
		V0_8_2_0,
		V0_8_2_1,
		V0_8_2_2,
		V0_9_0_0,
		V0_9_0_1,
		V0_10_0_0,
		V0_10_0_1,
		V0_10_1_0,
		V0_10_1_1,
		V0_10_2_0,
		V0_10_2_1,
		V0_10_2_2,
		V0_11_0_0,
		V0_11_0_1,
		V0_11_0_2,
		V1_0_0_0,
		V1_0_1_0,
		V1_0_2_0,
		V1_1_0_0,
		V1_1_1_0,
		V2_0_0_0,
		V2_0_1_0,
		V2_1_0_0,
		V2_1_1_0,
		V2_2_0_0,
		V2_2_1_0,
		V2_2_2_0,
		V2_3_0_0,
		V2_3_1_0,
		V2_4_0_0,
		V2_4_1_0,
		V2_5_0_0,
		V2_5_1_0,
		V2_6_0_0,
		V2_6_1_0,
		V2_6_2_0,
		V2_7_0_0,
		V2_7_1_0,
		V2_8_0_0,
		V2_8_1_0,
		V2_8_2_0,
		V3_0_0_0,
		V3_0_1_0,
		V3_0_2_0,
		V3_1_0_0,
		V3_1_1_0,
		V3_1_2_0,
		V3_2_0_0,
		V3_2_1_0,
		V3_2_2_0,
		V3_2_3_0,
		V3_3_0_0,
		V3_3_1_0,
		V3_3_2_0,
	}
	MinVersion     = V0_8_2_0
	MaxVersion     = V3_3_2_0
	DefaultVersion = V1_0_0_0

	// reduced set of versions to matrix test
	fvtRangeVersions = []KafkaVersion{
		V0_8_2_2,
		V0_10_2_2,
		V1_0_2_0,
		V1_1_1_0,
		V2_0_1_0,
		V2_2_2_0,
		V2_4_1_0,
		V2_6_2_0,
		V2_8_2_0,
		V3_1_2_0,
		V3_2_3_0,
		V3_3_2_0,
	}
)
