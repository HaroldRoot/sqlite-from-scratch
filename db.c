#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
    uint32_t id;
    /**
     * C 字符串应该以 null 字符结尾，解决方案是分配一个额外的字节
     */
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;  // only used by insert statement 仅供插入语句使用
} Statement;

/**
 * define the compact representation of a row
 * 定义行的紧凑表示形式
 */
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/**
 * a Table structure that points to pages of rows and keeps track of how many rows there are
 * 一个表结构，它指向行页并记录有多少行
 */
/**
 * 4KB 与大多数计算机体系结构的虚拟内存系统中使用的页面大小相同。
 * 这意味着我们数据库中的一个页面对应于操作系统使用的一个页面。
 * 操作系统会将页面作为一个整体移入和移出内存，而不是将其分割开来。
 */
const uint32_t PAGE_SIZE = 4096;  // 2^12 页面大小 4KB
/**
 * 设置一个任意限制，即我们将分配 100 个页面。
 * 当我们切换到树形结构时，数据库的最大容量将只受限于文件的最大容量。
 * (尽管我们仍然会限制内存中同时保留的页面数量）
 */
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;

/**
 * 打印行
 */
void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

/**
 * convert to and from the compact representation
 * 与紧凑表示法之间的转换
 */
void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/**
 * 确定特定行在内存中的读/写位置
 */
void* row_slot(Table* table, uint32_t row_num) {
    // 计算行所在的页码
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    // 获取相应页的指针
    void* page = table->pages[page_num];
    // 如果该页尚未分配内存
    if (page == NULL) {
        // Allocate memory only when we try to access page
        // 只有当我们尝试访问页面时才分配内存
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    // 计算行在页内的偏移量
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    // 计算行在页内的字节偏移量
    uint32_t byte_offset = row_offset * ROW_SIZE;
    // 返回行的指针，即页的起始地址加上字节偏移量
    return page + byte_offset;
}

/**
 * 初始化表
 */
Table* new_table() {
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }
    return table;
}

/**
 * 相应的内存释放函数
 */
void free_table(Table* table) {
    for (int i = 0; table->pages[i]; i++) {
        free(table->pages[i]);
    }
    free(table);
}

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

/**
 * 向用户打印提示
 * 在读取每一行输入之前执行此操作
 */
void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer* input_buffer) {
    /**
     * ssize_t getline(char **lineptr, size_t *n, FILE *stream);
     * lineptr ：变量指针，用于指向包含读取行的缓冲区。
     * 如果设置为 NULL，它将被 getline 调用，因此即使命令失败，用户也应释放它。
     * n：指针，指向用于保存已分配缓冲区大小的变量。
     * stream ：要读取的输入流。我们将从标准输入流读取数据。
     * 返回值：读取的字节数，可能小于缓冲区的大小。
     *
     * 告诉 getline 将读取的行存储在 input_buffer->buffer，
     * 将分配的缓冲区大小存储在 input_buffer->buffer_length。
     * 将返回值存储在 input_buffer->input_length 中。
     *
     * 缓冲区开始时为空，因此 getline 会分配足够的内存来存放输入行，并使缓冲区指向它。
     */

    ssize_t bytes_read =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline 忽略尾部换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

/**
 * 释放为 InputBuffer * 实例和相应结构的缓冲区元素分配的内存
 * （getline 在 read_input 中为 input_buffer->buffer 分配内存）
 */
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;

    /**
     * 在输入缓冲区上连续调用 strtok，
     * 每当到达一个分隔符（在我们的例子中是空格）时，
     * 就插入一个空字符，从而将输入缓冲区分成子串。
     * 它会返回一个指向子串起点的指针。
     */
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    /**
     * 对每个文本值调用 strlen() 来查看是否过长
     */
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * 让 execute_statement 读/写我们的表结构
 */
ExecuteResult execute_insert(Statement* statement, Table* table) {
    // 检查表是否已满
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    // 获取要插入的行的指针
    Row* row_to_insert = &(statement->row_to_insert);
    // 序列化并将行数据插入表中
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    // 更新表中的行数
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    // 创建用于存储从表中检索的行数据的临时变量
    Row row;
    // 遍历表中的每一行
    for (uint32_t i = 0; i < table->num_rows; i++) {
        // 反序列化并获取当前行的数据
        deserialize_row(row_slot(table, i), &row);
        // 打印当前行的数据
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
    // 根据语句类型执行相应的操作
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char* argv[]) {
    Table* table = new_table();
    InputBuffer* input_buffer = new_input_buffer();
    // 无限循环
    while (true) {
        print_prompt();            // 打印提示
        read_input(input_buffer);  // 获取一行输入

        /**
         * 像 .exit 这样的非 SQL 语句被称为 "元命令"。
         * 它们都以点开头，因此要检查它们，并在一个单独的函数中处理它们。
         */
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n",
                       input_buffer->buffer);
                continue;
        }

        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
        }
    }
}