package sql

import (
	"fmt"
	"strings"
)

type Parser struct {
	lexer  *Lexer
	tokens []Token
	pos    int
}

func NewParser(lexer *Lexer) *Parser {
	tokens, _ := lexer.Tokenize()
	return &Parser{
		lexer:  lexer,
		tokens: tokens,
		pos:    0,
	}
}

func (p *Parser) Parse() (Node, error) {
	if p.pos >= len(p.tokens) {
		return nil, NewParseError("unexpected end of input", p.currentToken(), "check your SQL statement")
	}

	tok := p.currentToken()

	switch tok.Type {
	case TokenKeyword:
		switch strings.ToUpper(tok.Value) {
		case "SELECT":
			return p.parseSelect()
		case "INSERT":
			return p.parseInsert()
		case "UPDATE":
			return p.parseUpdate()
		case "DELETE":
			return p.parseDelete()
		case "CREATE":
			return p.parseCreateTable()
		case "DROP":
			return p.parseDropTable()
		case "BEGIN":
			return p.parseBeginTransaction()
		case "COMMIT":
			return &CommitStatement{}, nil
		case "ROLLBACK":
			return &RollbackStatement{}, nil
		default:
			return nil, NewParseError(fmt.Sprintf("unexpected keyword: %s", tok.Value), tok, "check SQL syntax")
		}
	default:
		return nil, NewParseError(fmt.Sprintf("unexpected token: %s", tok.Value), tok, "expected a SQL keyword")
	}
}

func (p *Parser) currentToken() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) peekToken() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos+1]
}

func (p *Parser) advance() Token {
	tok := p.currentToken()
	p.pos++
	return tok
}

func (p *Parser) expectKeyword(keyword string) error {
	tok := p.currentToken()
	if tok.Type != TokenKeyword || !strings.EqualFold(tok.Value, keyword) {
		return NewParseError(fmt.Sprintf("expected keyword %s", keyword), tok,
			fmt.Sprintf("make sure to include %s", keyword))
	}
	p.advance()
	return nil
}

func (p *Parser) expectPunctuation(punct string) error {
	tok := p.currentToken()
	if tok.Type != TokenPunctuation || tok.Value != punct {
		return NewParseError(fmt.Sprintf("expected punctuation '%s'", punct), tok,
			fmt.Sprintf("add '%s'", punct))
	}
	p.advance()
	return nil
}

func (p *Parser) parseSelect() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}

	if p.peekToken().Type == TokenKeyword && strings.ToUpper(p.peekToken().Value) == "DISTINCT" {
		stmt.Distinct = true
		p.advance()
	}

	columns, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	tables, err := p.parseTableList()
	if err != nil {
		return nil, err
	}
	stmt.Tables = tables

	for {
		tok := p.currentToken()
		if tok.Type == TokenEOF || tok.Type == TokenPunctuation && tok.Value == ";" {
			break
		}

		if tok.Type == TokenKeyword {
			keyword := strings.ToUpper(tok.Value)
			switch keyword {
			case "WHERE":
				p.advance()
				expr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				stmt.Where = expr
			case "JOIN", "INNER", "LEFT", "RIGHT":
				join, err := p.parseJoin()
				if err != nil {
					return nil, err
				}
				stmt.Joins = append(stmt.Joins, join)
			case "ORDER":
				p.advance()
				if err := p.expectKeyword("BY"); err != nil {
					return nil, err
				}
				orderBys, err := p.parseOrderBy()
				if err != nil {
					return nil, err
				}
				stmt.OrderBy = orderBys
			case "LIMIT":
				p.advance()
				limit, err := p.parseIntegerLiteral()
				if err != nil {
					return nil, err
				}
				stmt.Limit = &limit
			case "OFFSET":
				p.advance()
				offset, err := p.parseIntegerLiteral()
				if err != nil {
					return nil, err
				}
				stmt.Offset = &offset
			default:
				break
			}
		} else {
			break
		}
	}

	return stmt, nil
}

func (p *Parser) parseColumnList() ([]string, error) {
	columns := make([]string, 0)

	if p.currentToken().Value == "*" {
		columns = append(columns, "*")
		p.advance()
		return columns, nil
	}

	for {
		tok := p.currentToken()
		if tok.Type == TokenIdentifier {
			colName := tok.Value
			p.advance()

			if p.currentToken().Type == TokenPunctuation && p.currentToken().Value == "." {
				p.advance()
				nextTok := p.currentToken()
				if nextTok.Type == TokenIdentifier {
					colName += "." + nextTok.Value
					p.advance()
				} else {
					return nil, NewParseError("expected column name after '.'", nextTok, "provide a valid column name")
				}
			}

			columns = append(columns, colName)

			if p.currentToken().Value == "," {
				p.advance()
			} else {
				break
			}
		} else {
			return nil, NewParseError("expected column name or *", tok, "provide valid column names")
		}
	}

	return columns, nil
}

func (p *Parser) parseTableList() ([]TableRef, error) {
	tables := make([]TableRef, 0)

	for {
		tok := p.currentToken()
		if tok.Type == TokenIdentifier {
			ref := TableRef{Name: tok.Value}
			p.advance()

			// Check for optional alias
			if p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "AS" {
				p.advance()
				aliasTok := p.currentToken()
				if aliasTok.Type == TokenIdentifier {
					ref.Alias = aliasTok.Value
					p.advance()
				} else {
					return nil, NewParseError("expected alias identifier after AS", aliasTok, "provide alias name")
				}
			} else if p.currentToken().Type == TokenIdentifier {
				// Implicit alias (e.g., "users u")
				// Ensure it's not a keyword that might start the next clause (though keywords should be TokenKeyword)
				ref.Alias = p.currentToken().Value
				p.advance()
			}

			tables = append(tables, ref)

			if p.currentToken().Value == "," {
				p.advance()
			} else {
				break
			}
		} else {
			return nil, NewParseError("expected table name", tok, "provide a valid table name")
		}
	}

	return tables, nil
}

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOrExpression()
}

func (p *Parser) parseOrExpression() (Expression, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "OR" {
		p.advance()
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpression{Left: left, Op: "OR", Right: right}
	}

	return left, nil
}

func (p *Parser) parseAndExpression() (Expression, error) {
	left, err := p.parseNotExpression()
	if err != nil {
		return nil, err
	}

	for p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "AND" {
		p.advance()
		right, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpression{Left: left, Op: "AND", Right: right}
	}

	return left, nil
}

func (p *Parser) parseNotExpression() (Expression, error) {
	if p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "NOT" {
		p.advance()
		expr, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		return &UnaryExpression{Op: "NOT", Right: expr}, nil
	}
	return p.parseComparisonExpression()
}

func (p *Parser) parseComparisonExpression() (Expression, error) {
	left, err := p.parseAdditiveExpression()
	if err != nil {
		return nil, err
	}

	tok := p.currentToken()
	if tok.Type == TokenOperator {
		op := tok.Value
		p.advance()
		right, err := p.parseAdditiveExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpression{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAdditiveExpression() (Expression, error) {
	left, err := p.parseMultiplicativeExpression()
	if err != nil {
		return nil, err
	}

	for {
		tok := p.currentToken()
		if tok.Type == TokenOperator && (tok.Value == "+" || tok.Value == "-") {
			op := tok.Value
			p.advance()
			right, err := p.parseMultiplicativeExpression()
			if err != nil {
				return nil, err
			}
			left = &BinaryExpression{Left: left, Op: op, Right: right}
		} else {
			break
		}
	}

	return left, nil
}

func (p *Parser) parseMultiplicativeExpression() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	for {
		tok := p.currentToken()
		if tok.Type == TokenOperator && (tok.Value == "*" || tok.Value == "/") {
			op := tok.Value
			p.advance()
			right, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, err
			}
			left = &BinaryExpression{Left: left, Op: op, Right: right}
		} else {
			break
		}
	}

	return left, nil
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	tok := p.currentToken()

	switch tok.Type {
	case TokenIdentifier:
		p.advance()
		colRef := &ColumnRef{Column: tok.Value}

		if p.currentToken().Value == "." {
			p.advance()
			nextTok := p.currentToken()
			if nextTok.Type == TokenIdentifier {
				colRef.Table = colRef.Column
				colRef.Column = nextTok.Value
				p.advance()
			}
		}

		return colRef, nil

	case TokenLiteral, TokenString:
		p.advance()
		return &LiteralExpression{Value: tok.Value}, nil

	case TokenKeyword:
		if strings.ToUpper(tok.Value) == "NULL" {
			p.advance()
			return &NullLiteral{}, nil
		}
		fallthrough

	case TokenPunctuation:
		if tok.Value == "(" {
			p.advance()
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			if err := p.expectPunctuation(")"); err != nil {
				return nil, err
			}
			return expr, nil
		}

		return nil, NewParseError(fmt.Sprintf("unexpected token: %s", tok.Value), tok, "check expression syntax")

	default:
		return nil, NewParseError("expected expression", tok, "provide a valid expression")
	}
}

func (p *Parser) parseJoin() (*JoinClause, error) {
	join := &JoinClause{}

	tok := p.currentToken()
	if tok.Type == TokenKeyword {
		join.Type = strings.ToUpper(tok.Value)
		p.advance()
	} else {
		return nil, NewParseError("expected join type", tok, "specify INNER, LEFT, or RIGHT JOIN")
	}

	if join.Type == "LEFT" || join.Type == "RIGHT" {
		if err := p.expectKeyword("JOIN"); err != nil {
			return nil, err
		}
	}

	tableTok := p.currentToken()
	if tableTok.Type != TokenIdentifier {
		return nil, NewParseError("expected table name", tableTok, "provide a valid table name")
	}
	join.Table = tableTok.Value
	p.advance()

	if p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "AS" {
		p.advance()
		aliasTok := p.currentToken()
		if aliasTok.Type != TokenIdentifier {
			return nil, NewParseError("expected table alias", aliasTok, "provide a valid alias")
		}
		join.Alias = aliasTok.Value
		p.advance()
	} else if p.currentToken().Type == TokenIdentifier {
		join.Alias = p.currentToken().Value
		p.advance()
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}

	conditions := make([]Expression, 0)
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, expr)

		if p.currentToken().Type != TokenKeyword ||
			!strings.EqualFold(p.currentToken().Value, "AND") {
			break
		}
		p.advance()
	}

	join.Conditions = conditions
	return join, nil
}

func (p *Parser) parseOrderBy() ([]OrderByClause, error) {
	orderBys := make([]OrderByClause, 0)

	for {
		colTok := p.currentToken()
		if colTok.Type != TokenIdentifier {
			return nil, NewParseError("expected column name", colTok, "provide valid column for ORDER BY")
		}

		ob := OrderByClause{
			Column: colTok.Value,
			Asc:    true,
		}
		p.advance()

		nextTok := p.currentToken()
		if nextTok.Type == TokenKeyword {
			if strings.ToUpper(nextTok.Value) == "DESC" {
				ob.Asc = false
				p.advance()
			} else if strings.ToUpper(nextTok.Value) == "ASC" {
				p.advance()
			}
		}

		orderBys = append(orderBys, ob)

		if p.currentToken().Value != "," {
			break
		}
		p.advance()
	}

	return orderBys, nil
}

func (p *Parser) parseIntegerLiteral() (int, error) {
	tok := p.currentToken()
	if tok.Type != TokenLiteral {
		return 0, NewParseError("expected integer literal", tok, "provide a valid number")
	}

	var val int
	_, err := fmt.Sscanf(tok.Value, "%d", &val)
	if err != nil {
		return 0, NewParseError("invalid integer literal", tok, "provide a valid integer")
	}

	p.advance()
	return val, nil
}

func (p *Parser) parseInsert() (*InsertStatement, error) {
	stmt := &InsertStatement{}

	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}

	tableTok := p.currentToken()
	if tableTok.Type != TokenIdentifier {
		return nil, NewParseError("expected table name", tableTok, "provide a valid table name")
	}
	stmt.Table = tableTok.Value
	p.advance()

	if p.currentToken().Value == "(" {
		p.advance()
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
		if err := p.expectPunctuation(")"); err != nil {
			return nil, err
		}
	}

	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}

	values, err := p.parseValuesList()
	if err != nil {
		return nil, err
	}
	stmt.Values = values

	return stmt, nil
}

func (p *Parser) parseIdentifierList() ([]string, error) {
	identifiers := make([]string, 0)

	for {
		tok := p.currentToken()
		if tok.Type != TokenIdentifier {
			return nil, NewParseError("expected identifier", tok, "provide valid identifier")
		}

		identifiers = append(identifiers, tok.Value)
		p.advance()

		if p.currentToken().Value != "," {
			break
		}
		p.advance()
	}

	return identifiers, nil
}

func (p *Parser) parseValuesList() ([][]Expression, error) {
	valuesList := make([][]Expression, 0)

	for {
		if err := p.expectPunctuation("("); err != nil {
			return nil, err
		}

		exprs, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}

		if err := p.expectPunctuation(")"); err != nil {
			return nil, err
		}

		valuesList = append(valuesList, exprs)

		if p.currentToken().Value != "," {
			break
		}
		p.advance()
	}

	return valuesList, nil
}

func (p *Parser) parseExpressionList() ([]Expression, error) {
	exprs := make([]Expression, 0)

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		if p.currentToken().Value != "," {
			break
		}
		p.advance()
	}

	return exprs, nil
}

func (p *Parser) parseUpdate() (*UpdateStatement, error) {
	stmt := &UpdateStatement{}

	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}

	tableTok := p.currentToken()
	if tableTok.Type != TokenIdentifier {
		return nil, NewParseError("expected table name", tableTok, "provide a valid table name")
	}
	stmt.Table = tableTok.Value
	p.advance()

	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}

	setClauses, err := p.parseSetClauses()
	if err != nil {
		return nil, err
	}
	stmt.SetClauses = setClauses

	if p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "WHERE" {
		p.advance()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseSetClauses() ([]SetClause, error) {
	clauses := make([]SetClause, 0)

	for {
		colTok := p.currentToken()
		if colTok.Type != TokenIdentifier {
			return nil, NewParseError("expected column name", colTok, "provide valid column name")
		}

		col := colTok.Value
		p.advance()

		if p.currentToken().Type != TokenOperator || p.currentToken().Value != "=" {
			return nil, NewParseError("expected =", p.currentToken(), "use = for assignment")
		}
		p.advance()

		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		clauses = append(clauses, SetClause{Column: col, Value: expr})

		if p.currentToken().Value != "," {
			break
		}
		p.advance()
	}

	return clauses, nil
}

func (p *Parser) parseDelete() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}

	if err := p.expectKeyword("DELETE"); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	tableTok := p.currentToken()
	if tableTok.Type != TokenIdentifier {
		return nil, NewParseError("expected table name", tableTok, "provide a valid table name")
	}
	stmt.Table = tableTok.Value
	p.advance()

	if p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "WHERE" {
		p.advance()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseCreateTable() (*CreateTableStatement, error) {
	stmt := &CreateTableStatement{}

	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	tableTok := p.currentToken()
	if tableTok.Type != TokenIdentifier {
		return nil, NewParseError("expected table name", tableTok, "provide a valid table name")
	}
	stmt.Table = tableTok.Value
	p.advance()

	if err := p.expectPunctuation("("); err != nil {
		return nil, err
	}

	columns, err := p.parseColumnDefinitions()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	if err := p.expectPunctuation(")"); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseColumnDefinitions() ([]ColumnDefinition, error) {
	columns := make([]ColumnDefinition, 0)

	for {
		colTok := p.currentToken()
		if colTok.Type != TokenIdentifier {
			return nil, NewParseError("expected column name", colTok, "provide valid column name")
		}

		col := ColumnDefinition{Name: colTok.Value}
		p.advance()

		typeTok := p.currentToken()
		if typeTok.Type != TokenKeyword && typeTok.Type != TokenIdentifier {
			return nil, NewParseError("expected column type", typeTok, "specify INTEGER, TEXT, FLOAT, or BOOLEAN")
		}
		col.Type = strings.ToUpper(typeTok.Value)
		p.advance()

		for {
			tok := p.currentToken()
			if tok.Type == TokenPunctuation && tok.Value == ")" {
				break
			}
			if tok.Type == TokenPunctuation && tok.Value == "," {
				break
			}

			if tok.Type == TokenKeyword {
				keyword := strings.ToUpper(tok.Value)
				switch keyword {
				case "PRIMARY":
					p.advance()
					if strings.ToUpper(p.currentToken().Value) != "KEY" {
						return nil, NewParseError("expected KEY after PRIMARY", p.currentToken(), "use PRIMARY KEY")
					}
					p.advance()
					col.Primary = true
				case "UNIQUE":
					p.advance()
					col.Unique = true
				case "NOT":
					p.advance()
					if strings.ToUpper(p.currentToken().Value) != "NULL" {
						return nil, NewParseError("expected NULL after NOT", p.currentToken(), "use NOT NULL")
					}
					p.advance()
					col.NotNull = true
				case "DEFAULT":
					p.advance()
					expr, err := p.parsePrimaryExpression()
					if err != nil {
						return nil, err
					}
					col.Default = &expr
				default:
					break
				}
			} else {
				break
			}
		}

		columns = append(columns, col)

		if p.currentToken().Value != "," {
			break
		}
		p.advance()
	}

	return columns, nil
}

func (p *Parser) parseDropTable() (*DropTableStatement, error) {
	stmt := &DropTableStatement{}

	if err := p.expectKeyword("DROP"); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	tableTok := p.currentToken()
	if tableTok.Type != TokenIdentifier {
		return nil, NewParseError("expected table name", tableTok, "provide a valid table name")
	}
	stmt.Table = tableTok.Value
	p.advance()

	return stmt, nil
}

func (p *Parser) parseBeginTransaction() (*BeginTransactionStatement, error) {
	if err := p.expectKeyword("BEGIN"); err != nil {
		return nil, err
	}

	if p.currentToken().Type == TokenKeyword && strings.ToUpper(p.currentToken().Value) == "TRANSACTION" {
		p.advance()
	}

	return &BeginTransactionStatement{}, nil
}
