package proxylog

import core "dappco.re/go"

func pathJoin(parts ...string) string {
	return core.PathJoin(parts...)
}

func readFile(path string) ([]byte, error) {
	r := core.ReadFile(path)
	if !r.OK {
		return nil, core.NewError(r.Error())
	}
	return r.Value.([]byte), nil
}

func statFile(path string) (core.FsFileInfo, error) {
	r := core.Stat(path)
	if !r.OK {
		return nil, core.NewError(r.Error())
	}
	return r.Value.(core.FsFileInfo), nil
}

func containsString(value, needle string) bool {
	return core.Contains(value, needle)
}

func containsAnyString(value, chars string) bool {
	for _, r := range value {
		for _, c := range chars {
			if r == c {
				return true
			}
		}
	}
	return false
}

func trimSpaceString(value string) string {
	return core.Trim(value)
}
