package sqlh

func _panic(err error) {
	if err != nil {
		panic(err)
	}
}
