package roll

type buffer struct {
  datadir string
}

func (b *buffer) Write(bs []byte) (int, error) {
  return 0, nil
}

func (b *buffer) Close() error {
  return nil
}
