package main

import (
	"fmt"
	"net/http"
	"strconv"
	"text/template"

	"github.com/mryan-3/rdbms/internal/sql"
	"github.com/mryan-3/rdbms/internal/storage"
)

var db *storage.Database
var exec *sql.Executor

func main() {
	db = storage.NewDatabase()
	exec = sql.NewExecutor(db)

	initSchema()

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/favicon.ico", handleFavicon)
	http.HandleFunc("/users", handleUsers)
	http.HandleFunc("/tasks", handleTasks)
	http.HandleFunc("/users/new", handleUserForm)
	http.HandleFunc("/tasks/new", handleTaskForm)
	http.HandleFunc("/users/create", handleCreateUser)
	http.HandleFunc("/tasks/create", handleCreateTask)
	http.HandleFunc("/users/edit", handleEditUserForm)
	http.HandleFunc("/tasks/edit", handleEditTaskForm)
	http.HandleFunc("/users/update", handleUpdateUser)
	http.HandleFunc("/tasks/update", handleUpdateTask)
	http.HandleFunc("/users/delete", handleDeleteUser)
	http.HandleFunc("/tasks/delete", handleDeleteTask)
	http.HandleFunc("/static/style.css", handleStyleCSS)

	fmt.Println("Server starting on http://localhost:8080")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()
	http.ListenAndServe(":8080", nil)
}

func initSchema() {
	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);",
		"CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, description TEXT, status TEXT DEFAULT 'pending', user_id INTEGER);",
		"INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');",
		"INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane@example.com');",
		"INSERT INTO tasks (id, title, description, status, user_id) VALUES (1, 'Complete project', 'Finish RDBMS implementation', 'in_progress', 1);",
		"INSERT INTO tasks (id, title, description, status, user_id) VALUES (2, 'Review code', 'Review pull request', 'pending', 2);",
	}

	for _, stmt := range statements {
		_, err := executeSQLWithResult(stmt)
		if err != nil {
			fmt.Printf("Error initializing schema: %v\n", err)
		}
	}

	fmt.Println("Database initialized with sample data")
	fmt.Println()
}

func executeSQL(stmt string) {
	lexer := sql.NewLexer(stmt)
	parser := sql.NewParser(lexer)

	node, err := parser.Parse()
	if err != nil {
		fmt.Printf("Error parsing SQL: %v\n", err)
		return
	}

	_, err = exec.Execute(node)
	if err != nil {
		fmt.Printf("Error executing SQL: %v\n", err)
	}
}

func executeSQLWithResult(stmt string) (*sql.Result, error) {
	lexer := sql.NewLexer(stmt)
	parser := sql.NewParser(lexer)

	node, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	return exec.Execute(node)
}

type User struct {
	ID    int
	Name  string
	Email string
}

type Task struct {
	ID          int
	Title       string
	Description string
	Status      string
	UserID      int
}

type TaskWithUser struct {
	ID          int
	Title       string
	Description string
	Status      string
	UserID      int
	UserName    string
	UserEmail   string
	StatusClass string
}

func getUsers() []User {
	result, err := executeSQLWithResult("SELECT id, name, email FROM users")
	if err != nil {
		fmt.Printf("Error getting users: %v\n", err)
		return []User{}
	}

	users := make([]User, 0)

	for _, row := range result.Rows {
		id, _ := strconv.Atoi(row[0])
		users = append(users, User{
			ID:    id,
			Name:  row[1],
			Email: row[2],
		})
	}

	return users
}

func getTasks() []Task {
	result, err := executeSQLWithResult("SELECT id, title, description, status, user_id FROM tasks")
	if err != nil {
		fmt.Printf("Error getting tasks: %v\n", err)
		return []Task{}
	}

	tasks := make([]Task, 0)

	for _, row := range result.Rows {
		id, _ := strconv.Atoi(row[0])
		userID, _ := strconv.Atoi(row[4])
		tasks = append(tasks, Task{
			ID:          id,
			Title:       row[1],
			Description: row[2],
			Status:      row[3],
			UserID:      userID,
		})
	}

	return tasks
}

func getTasksWithUsers() []TaskWithUser {
	result, err := executeSQLWithResult("SELECT t.id, t.title, t.description, t.status, t.user_id, u.name, u.email FROM tasks t LEFT JOIN users u ON t.user_id = u.id")

	if err != nil {
		fmt.Printf("Error getting tasks with users: %v\n", err)
		return []TaskWithUser{}
	}

	tasks := make([]TaskWithUser, 0)

	for _, row := range result.Rows {
		id, _ := strconv.Atoi(row[0])
		userID, _ := strconv.Atoi(row[4])

		statusClass := "pending"
		if row[3] == "in_progress" {
			statusClass = "in_progress"
		} else if row[3] == "completed" {
			statusClass = "completed"
		}

		userName := "Unassigned"
		if len(row) > 5 && row[5] != "" {
			userName = row[5]
		}

		userEmail := ""
		if len(row) > 6 {
			userEmail = row[6]
		}

		tasks = append(tasks, TaskWithUser{
			ID:          id,
			Title:       row[1],
			Description: row[2],
			Status:      row[3],
			UserID:      userID,
			UserName:    userName,
			UserEmail:   userEmail,
			StatusClass: statusClass,
		})
	}

	return tasks
}

func handleIndex(w http.ResponseWriter, req *http.Request) {
	users := getUsers()
	tasks := getTasksWithUsers()

	tmpl := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Task Manager - RDBMS Demo</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <h1>Task Manager</h1>
        <p class="subtitle">Built with RDBMS - A simple relational database management system</p>

        <div class="section">
            <h2>Users</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Name</th>
                        <th>Email</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Users}}
                    <tr>
                        <td>{{.ID}}</td>
                        <td>{{.Name}}</td>
                        <td>{{.Email}}</td>
                        <td>
                            <a href="/users/edit?id={{.ID}}">Edit</a> |
                            <a href="/users/delete?id={{.ID}}" onclick="return confirm('Are you sure?')">Delete</a>
                        </td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
            <a href="/users/new" class="btn">Add User</a>
        </div>

        <div class="section">
            <h2>Tasks</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Description</th>
                        <th>Status</th>
                        <th>Assigned To</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Tasks}}
                    <tr>
                        <td>{{.ID}}</td>
                        <td>{{.Title}}</td>
                        <td>{{.Description}}</td>
                        <td><span class="status {{.StatusClass}}">{{.Status}}</span></td>
                        <td>{{.UserName}}</td>
                        <td>
                            <a href="/tasks/edit?id={{.ID}}">Edit</a> |
                            <a href="/tasks/delete?id={{.ID}}" onclick="return confirm('Are you sure?')">Delete</a>
                        </td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
            <a href="/tasks/new" class="btn">Add Task</a>
        </div>

        <div class="section">
            <h2>Database Info</h2>
            <pre>{{.DBInfo}}</pre>
        </div>
    </div>
</body>
</html>`

	t, _ := template.New("index").Parse(tmpl)
	dbInfo := fmt.Sprintf("Tables: %d\nTotal Users: %d\nTotal Tasks: %d",
		len(db.ListTables()), len(users), len(tasks))
	data := struct {
		Users  []User
		Tasks  []TaskWithUser
		DBInfo string
	}{
		Users:  users,
		Tasks:  tasks,
		DBInfo: dbInfo,
	}
	t.Execute(w, data)
}

func handleUsers(w http.ResponseWriter, req *http.Request) {
	users := getUsers()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[")
	for i, user := range users {
		if i > 0 {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "{\"id\":%d,\"name\":\"%s\",\"email\":\"%s\"}", user.ID, user.Name, user.Email)
	}
	fmt.Fprintf(w, "]")
}

func handleTasks(w http.ResponseWriter, req *http.Request) {
	tasks := getTasks()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "[")
	for i, task := range tasks {
		if i > 0 {
			fmt.Fprintf(w, ",")
		}
		fmt.Fprintf(w, "{\"id\":%d,\"title\":\"%s\",\"description\":\"%s\",\"status\":\"%s\",\"user_id\":%d}",
			task.ID, task.Title, task.Description, task.Status, task.UserID)
	}
	fmt.Fprintf(w, "]")
}

func handleUserForm(w http.ResponseWriter, req *http.Request) {
	tmpl := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Add User - RDBMS Demo</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <h1>Add User</h1>
        <form method="POST" action="/users/create">
            <div class="form-group">
                <label for="name">Name:</label>
                <input type="text" id="name" name="name" required>
            </div>
            <div class="form-group">
                <label for="email">Email:</label>
                <input type="email" id="email" name="email" required>
            </div>
            <div class="form-group">
                <button type="submit" class="btn">Create User</button>
                <a href="/" class="btn btn-secondary">Cancel</a>
            </div>
        </form>
    </div>
</body>
</html>`

	t, _ := template.New("user_form").Parse(tmpl)
	t.Execute(w, nil)
}

func handleTaskForm(w http.ResponseWriter, req *http.Request) {
	users := getUsers()

	tmpl := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Add Task - RDBMS Demo</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <h1>Add Task</h1>
        <form method="POST" action="/tasks/create">
            <div class="form-group">
                <label for="title">Title:</label>
                <input type="text" id="title" name="title" required>
            </div>
            <div class="form-group">
                <label for="description">Description:</label>
                <textarea id="description" name="description"></textarea>
            </div>
            <div class="form-group">
                <label for="status">Status:</label>
                <select id="status" name="status">
                    <option value="pending">Pending</option>
                    <option value="in_progress">In Progress</option>
                    <option value="completed">Completed</option>
                </select>
            </div>
            <div class="form-group">
                <label for="user_id">Assign to:</label>
                <select id="user_id" name="user_id">
                    <option value="">Unassigned</option>
                    {{range .Users}}
                    <option value="{{.ID}}">{{.Name}} ({{.Email}})</option>
                    {{end}}
                </select>
            </div>
            <div class="form-group">
                <button type="submit" class="btn">Create Task</button>
                <a href="/" class="btn btn-secondary">Cancel</a>
            </div>
        </form>
    </div>
</body>
</html>`

	t, _ := template.New("task_form").Parse(tmpl)
	t.Execute(w, struct{ Users []User }{users})
}

func handleCreateUser(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Redirect(w, req, "/users/new", http.StatusSeeOther)
		return
	}

	name := req.FormValue("name")
	email := req.FormValue("email")

	stmt := fmt.Sprintf("INSERT INTO users (name, email) VALUES ('%s', '%s')", name, email)
	executeSQL(stmt)

	http.Redirect(w, req, "/", http.StatusSeeOther)
}

func handleCreateTask(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Redirect(w, req, "/tasks/new", http.StatusSeeOther)
		return
	}

	title := req.FormValue("title")
	description := req.FormValue("description")
	status := req.FormValue("status")
	userID := req.FormValue("user_id")

	var stmt string
	if userID == "" {
		stmt = fmt.Sprintf("INSERT INTO tasks (title, description, status) VALUES ('%s', '%s', '%s')",
			title, description, status)
	} else {
		stmt = fmt.Sprintf("INSERT INTO tasks (title, description, status, user_id) VALUES ('%s', '%s', '%s', %s)",
			title, description, status, userID)
	}

	executeSQL(stmt)

	http.Redirect(w, req, "/", http.StatusSeeOther)
}

func getUser(id string) (*User, error) {
	stmt := fmt.Sprintf("SELECT id, name, email FROM users WHERE id = %s", id)
	result, err := executeSQLWithResult(stmt)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("user not found")
	}

	row := result.Rows[0]
	userID, _ := strconv.Atoi(row[0])
	return &User{
		ID:    userID,
		Name:  row[1],
		Email: row[2],
	}, nil
}

func handleEditUserForm(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	user, err := getUser(id)
	if err != nil {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	tmpl := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Edit User - RDBMS Demo</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <h1>Edit User</h1>
        <form method="POST" action="/users/update">
            <input type="hidden" name="id" value="{{.ID}}">
            <div class="form-group">
                <label for="name">Name:</label>
                <input type="text" id="name" name="name" value="{{.Name}}" required>
            </div>
            <div class="form-group">
                <label for="email">Email:</label>
                <input type="email" id="email" name="email" value="{{.Email}}" required>
            </div>
            <div class="form-group">
                <button type="submit" class="btn">Update User</button>
                <a href="/" class="btn btn-secondary">Cancel</a>
            </div>
        </form>
    </div>
</body>
</html>`

	t, _ := template.New("edit_user").Parse(tmpl)
	t.Execute(w, user)
}

func handleUpdateUser(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Redirect(w, req, "/", http.StatusSeeOther)
		return
	}

	id := req.FormValue("id")
	name := req.FormValue("name")
	email := req.FormValue("email")

	stmt := fmt.Sprintf("UPDATE users SET name = '%s', email = '%s' WHERE id = %s", name, email, id)
	executeSQL(stmt)

	http.Redirect(w, req, "/", http.StatusSeeOther)
}

func getTask(id string) (*Task, error) {
	stmt := fmt.Sprintf("SELECT id, title, description, status, user_id FROM tasks WHERE id = %s", id)
	result, err := executeSQLWithResult(stmt)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("task not found")
	}

	row := result.Rows[0]
	taskID, _ := strconv.Atoi(row[0])
	userID, _ := strconv.Atoi(row[4])

	return &Task{
		ID:          taskID,
		Title:       row[1],
		Description: row[2],
		Status:      row[3],
		UserID:      userID,
	}, nil
}

func handleEditTaskForm(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	task, err := getTask(id)
	if err != nil {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	users := getUsers()

	tmpl := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Edit Task - RDBMS Demo</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <h1>Edit Task</h1>
        <form method="POST" action="/tasks/update">
            <input type="hidden" name="id" value="{{.Task.ID}}">
            <div class="form-group">
                <label for="title">Title:</label>
                <input type="text" id="title" name="title" value="{{.Task.Title}}" required>
            </div>
            <div class="form-group">
                <label for="description">Description:</label>
                <textarea id="description" name="description">{{.Task.Description}}</textarea>
            </div>
            <div class="form-group">
                <label for="status">Status:</label>
                <select id="status" name="status">
                    <option value="pending" {{if eq .Task.Status "pending"}}selected{{end}}>Pending</option>
                    <option value="in_progress" {{if eq .Task.Status "in_progress"}}selected{{end}}>In Progress</option>
                    <option value="completed" {{if eq .Task.Status "completed"}}selected{{end}}>Completed</option>
                </select>
            </div>
            <div class="form-group">
                <label for="user_id">Assign to:</label>
                <select id="user_id" name="user_id">
                    <option value="">Unassigned</option>
                    {{range .Users}}
                    <option value="{{.ID}}" {{if eq .ID $.Task.UserID}}selected{{end}}>{{.Name}} ({{.Email}})</option>
                    {{end}}
                </select>
            </div>
            <div class="form-group">
                <button type="submit" class="btn">Update Task</button>
                <a href="/" class="btn btn-secondary">Cancel</a>
            </div>
        </form>
    </div>
</body>
</html>`

	t, _ := template.New("edit_task").Parse(tmpl)
	data := struct {
		Task  *Task
		Users []User
	}{
		Task:  task,
		Users: users,
	}
	t.Execute(w, data)
}

func handleUpdateTask(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Redirect(w, req, "/", http.StatusSeeOther)
		return
	}

	id := req.FormValue("id")
	title := req.FormValue("title")
	description := req.FormValue("description")
	status := req.FormValue("status")
	userID := req.FormValue("user_id")

	var stmt string
	if userID == "" {
		stmt = fmt.Sprintf("UPDATE tasks SET title = '%s', description = '%s', status = '%s', user_id = NULL WHERE id = %s",
			title, description, status, id)
	} else {
		stmt = fmt.Sprintf("UPDATE tasks SET title = '%s', description = '%s', status = '%s', user_id = %s WHERE id = %s",
			title, description, status, userID, id)
	}

	executeSQL(stmt)

	http.Redirect(w, req, "/", http.StatusSeeOther)
}

func handleDeleteUser(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	stmt := fmt.Sprintf("DELETE FROM users WHERE id = %s", id)
	executeSQL(stmt)

	http.Redirect(w, req, "/", http.StatusSeeOther)
}

func handleDeleteTask(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	stmt := fmt.Sprintf("DELETE FROM tasks WHERE id = %s", id)
	executeSQL(stmt)

	http.Redirect(w, req, "/", http.StatusSeeOther)
}

func handleFavicon(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func handleStyleCSS(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/css")
	fmt.Fprint(w, `
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
    line-height: 1.6;
    padding: 20px;
    background-color: #f5f5f5;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    background-color: white;
    padding: 30px;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

h1 {
    color: #333;
    margin-bottom: 5px;
    font-size: 2em;
}

.subtitle {
    color: #666;
    margin-bottom: 30px;
    font-style: italic;
}

h2 {
    color: #555;
    margin: 20px 0 10px 0;
    border-bottom: 2px solid #007bff;
    padding-bottom: 5px;
}

.section {
    margin: 40px 0;
}

table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 20px;
}

table thead {
    background-color: #f8f9fa;
}

table th, table td {
    padding: 12px;
    text-align: left;
    border-bottom: 1px solid #ddd;
}

table tbody tr:hover {
    background-color: #f5f5f5;
}

.btn {
    display: inline-block;
    padding: 10px 20px;
    background-color: #007bff;
    color: white;
    text-decoration: none;
    border-radius: 4px;
    margin-right: 10px;
    transition: background-color 0.3s;
}

.btn:hover {
    background-color: #0056b3;
}

.btn-secondary {
    background-color: #6c757d;
}

.btn-secondary:hover {
    background-color: #545b62;
}

.form-group {
    margin-bottom: 20px;
}

.form-group label {
    display: block;
    margin-bottom: 8px;
    font-weight: bold;
    color: #333;
}

.form-group input,
.form-group textarea,
.form-group select {
    width: 100%;
    padding: 10px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 14px;
    transition: border-color 0.3s;
}

.form-group input:focus,
.form-group textarea:focus,
.form-group select:focus {
    outline: none;
    border-color: #007bff;
}

.form-group textarea {
    min-height: 100px;
    resize: vertical;
}

.status {
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: bold;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.status.pending {
    background-color: #ffc107;
    color: #000;
}

.status.in_progress {
    background-color: #17a2b8;
    color: white;
}

.status.completed {
    background-color: #28a745;
    color: white;
}

pre {
    background-color: #f8f9fa;
    padding: 15px;
    border-radius: 4px;
    overflow-x: auto;
}
`)
}
