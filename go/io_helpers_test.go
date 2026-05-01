package proxy

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

func writeFile(path string, data []byte, mode core.FileMode) error {
	r := core.WriteFile(path, data, mode)
	if !r.OK {
		return core.NewError(r.Error())
	}
	return nil
}

func mkdir(path string, mode core.FileMode) error {
	r := core.Mkdir(path, mode)
	if !r.OK {
		return core.NewError(r.Error())
	}
	return nil
}

func createFile(path string) (*core.OSFile, error) {
	r := core.Create(path)
	if !r.OK {
		return nil, core.NewError(r.Error())
	}
	return r.Value.(*core.OSFile), nil
}

func statFile(path string) (core.FsFileInfo, error) {
	r := core.Stat(path)
	if !r.OK {
		return nil, core.NewError(r.Error())
	}
	return r.Value.(core.FsFileInfo), nil
}
