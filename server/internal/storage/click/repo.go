package click

type ClickRepo struct {
	conn *ClickClient
}

func NewClickRepo(conn *ClickClient) *ClickRepo {
	return &ClickRepo{conn: conn}
}

func (cc *ClickRepo) WriteAddr() error {

	return nil
}
