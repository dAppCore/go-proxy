package pool

import core "dappco.re/go"

func testJSONMarshal(value any) ([]byte, error) {
	r := core.JSONMarshal(value)
	if !r.OK {
		return nil, core.NewError(r.Error())
	}
	return r.Value.([]byte), nil
}

func testJSONUnmarshal(data []byte, target any) error {
	r := core.JSONUnmarshal(data, target)
	if !r.OK {
		return core.NewError(r.Error())
	}
	return nil
}
