package version

var (
	// The git commit that was compiled. This will be filled in by the compiler.
	GitCommit   string
	GitDescribe string

	// The main version number that is being run at the moment.
	Version = "1.0.0"

	// A pre-release marker for the version. If this is "" (empty string)
	// then it means that it is a final release. Otherwise, this is a pre-release
	// such as "dev" (in development), "beta", "rc1", etc.
	VersionPrerelease = "dev"

	// VersionMetadata is metadata further describing the build type.
	VersionMetadata = ""
)

// VersionInfo
type VersionInfo struct {
	Revision          string
	Version           string
	VersionPrerelease string
	VersionMetadata   string
}

func GetVersion() *VersionInfo {
	ver := Version
	rel := VersionPrerelease
	md := VersionMetadata
	if GitDescribe != "" {
		ver = GitDescribe
	}
	if GitDescribe == "" && rel == "" && VersionPrerelease != "" {
		rel = "dev"
	}

	return &VersionInfo{
		Revision:          GitCommit,
		Version:           ver,
		VersionPrerelease: rel,
		VersionMetadata:   md,
	}
}
