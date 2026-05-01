package simple

import core "dappco.re/go"

func testJSONUnmarshal(data []byte, target any) error {
	r := core.JSONUnmarshal(data, target)
	if !r.OK {
		return core.NewError(r.Error())
	}
	return nil
}
