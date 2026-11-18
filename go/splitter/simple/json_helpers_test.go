package simple

import core "github.com/dappcore/go"

func testJSONUnmarshal(data []byte, target any) error {
	r := core.JSONUnmarshal(data, target)
	if !r.OK {
		return core.NewError(r.Error())
	}
	return nil
}
