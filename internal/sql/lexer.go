package sql

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenKeyword
	TokenIdentifier
	TokenLiteral
	TokenOperator
	TokenPunctuation
	TokenString
)

type Token struct {
	Type     TokenType
	Value    string
	Position Position
}

type Position struct {
	Line   int
	Column int
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           rune
	line         int
	column       int
	tokens       []Token
}

func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 1,
	}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = rune(l.input[l.readPosition])
	}
	l.position = l.readPosition
	l.readPosition++

	if l.ch == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
}

func (l *Lexer) peekChar() rune {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return rune(l.input[l.readPosition])
}

func (l *Lexer) skipWhitespace() {
	for unicode.IsSpace(l.ch) && l.ch != '\n' {
		l.readChar()
	}
}

func (l *Lexer) skipComment() {
	if l.ch == '-' && l.peekChar() == '-' {
		for l.ch != '\n' && l.ch != 0 {
			l.readChar()
		}
		l.skipWhitespace()
	}
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()
	l.skipComment()

	pos := Position{Line: l.line, Column: l.column}

	switch l.ch {
	case 0:
		tok = Token{Type: TokenEOF, Value: "", Position: pos}
	case '(':
		tok = Token{Type: TokenPunctuation, Value: "(", Position: pos}
		l.readChar()
	case ')':
		tok = Token{Type: TokenPunctuation, Value: ")", Position: pos}
		l.readChar()
	case ',':
		tok = Token{Type: TokenPunctuation, Value: ",", Position: pos}
		l.readChar()
	case '.':
		tok = Token{Type: TokenPunctuation, Value: ".", Position: pos}
		l.readChar()
	case ';':
		tok = Token{Type: TokenPunctuation, Value: ";", Position: pos}
		l.readChar()
	case '*':
		tok = Token{Type: TokenOperator, Value: "*", Position: pos}
		l.readChar()
	case '=':
		tok = Token{Type: TokenOperator, Value: "=", Position: pos}
		l.readChar()
	case '!':
		if l.peekChar() == '=' {
			tok = Token{Type: TokenOperator, Value: "!=", Position: pos}
			l.readChar()
			l.readChar()
		} else {
			tok = Token{Type: TokenOperator, Value: "!", Position: pos}
			l.readChar()
		}
	case '<':
		if l.peekChar() == '=' {
			tok = Token{Type: TokenOperator, Value: "<=", Position: pos}
			l.readChar()
			l.readChar()
		} else {
			tok = Token{Type: TokenOperator, Value: "<", Position: pos}
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			tok = Token{Type: TokenOperator, Value: ">=", Position: pos}
			l.readChar()
			l.readChar()
		} else {
			tok = Token{Type: TokenOperator, Value: ">", Position: pos}
			l.readChar()
		}
	case '\'':
		tok = Token{Type: TokenString, Value: l.readString(), Position: pos}
	default:
		if isLetter(l.ch) {
			ident := l.readIdentifier()
			if isKeyword(ident) {
				tok = Token{Type: TokenKeyword, Value: ident, Position: pos}
			} else {
				tok = Token{Type: TokenIdentifier, Value: ident, Position: pos}
			}
			return tok
		} else if isDigit(l.ch) {
			tok = Token{Type: TokenLiteral, Value: l.readNumber(), Position: pos}
			return tok
		} else {
			tok = Token{Type: TokenOperator, Value: string(l.ch), Position: pos}
			l.readChar()
		}
	}

	return tok
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	hasDecimal := false
	for isDigit(l.ch) || l.ch == '.' {
		if l.ch == '.' {
			if hasDecimal {
				break
			}
			hasDecimal = true
		}
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readString() string {
	position := l.position + 1
	l.readChar()

	for l.ch != '\'' && l.ch != 0 {
		if l.ch == '\\' && l.peekChar() == '\'' {
			l.readChar()
		}
		l.readChar()
	}

	value := l.input[position:l.position]
	l.readChar()
	return value
}

func isLetter(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

func isDigit(ch rune) bool {
	return unicode.IsDigit(ch)
}

func isKeyword(ident string) bool {
	keywords := map[string]bool{
		"SELECT":      true,
		"INSERT":      true,
		"UPDATE":      true,
		"DELETE":      true,
		"CREATE":      true,
		"DROP":        true,
		"TABLE":       true,
		"INTO":        true,
		"VALUES":      true,
		"SET":         true,
		"FROM":        true,
		"WHERE":       true,
		"JOIN":        true,
		"INNER":       true,
		"LEFT":        true,
		"RIGHT":       true,
		"ON":          true,
		"AND":         true,
		"OR":          true,
		"NOT":         true,
		"NULL":        true,
		"PRIMARY":     true,
		"KEY":         true,
		"UNIQUE":      true,
		"DEFAULT":     true,
		"FOREIGN":     true,
		"REFERENCES":  true,
		"CASCADE":     true,
		"RESTRICT":    true,
		"LIMIT":       true,
		"OFFSET":      true,
		"ORDER":       true,
		"BY":          true,
		"ASC":         true,
		"DESC":        true,
		"BEGIN":       true,
		"COMMIT":      true,
		"ROLLBACK":    true,
		"TRANSACTION": true,
	}
	return keywords[strings.ToUpper(ident)]
}

func (l *Lexer) Tokenize() ([]Token, error) {
	tokens := make([]Token, 0)

	for {
		tok := l.NextToken()
		if tok.Type == TokenEOF {
			break
		}
		tokens = append(tokens, tok)
	}

	return tokens, nil
}

type SQLError struct {
	Code       int
	Message    string
	Line       int
	Column     int
	Context    string
	Suggestion string
}

func (e *SQLError) Error() string {
	result := fmt.Sprintf("SQL error at line %d, column %d: %s", e.Line, e.Column, e.Message)
	if e.Context != "" {
		result += fmt.Sprintf("\nContext: %s", e.Context)
	}
	if e.Suggestion != "" {
		result += fmt.Sprintf("\nSuggestion: %s", e.Suggestion)
	}
	return result
}

func NewParseError(message string, token Token, suggestion string) *SQLError {
	return &SQLError{
		Code:       ErrSyntax,
		Message:    message,
		Line:       token.Position.Line,
		Column:     token.Position.Column,
		Context:    fmt.Sprintf("near '%s'", token.Value),
		Suggestion: suggestion,
	}
}

const (
	ErrSyntax = iota
	ErrTableNotFound
	ErrColumnNotFound
	ErrDuplicateKey
	ErrConstraintViolation
	ErrTypeMismatch
	ErrTransaction
	ErrUnknown
)
