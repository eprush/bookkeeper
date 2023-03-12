"""
Модуль содержит класс виджета расходов
для отображения информации о расходах таблицей.
"""
from typing import cast, List, Dict, Tuple

from PyQt6.QtCore import pyqtSignal, Qt
from PyQt6.QtWidgets import (
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QWidget,
    QVBoxLayout,
    QAbstractItemView,
    QHBoxLayout,
    QPushButton, QComboBox,
)


class ExpensesListWidget(QWidget):
    """
    Виджет для отображения таблицы расходов.
    """

    delete_button_clicked = pyqtSignal(int)
    category_cell_double_clicked = pyqtSignal(int, int, str)
    category_cell_changed = pyqtSignal(int, int, int)

    def __init__(self) -> None:
        super().__init__()

        self.table = QTableWidget()
        self.init_table()
        self.update_table([])

        day_button = QPushButton("День")
        week_button = QPushButton("Неделя")
        month_button = QPushButton("Месяц")

        filter_layout = QHBoxLayout()
        filter_layout.addWidget(day_button)
        filter_layout.addWidget(week_button)
        filter_layout.addWidget(month_button)

        layout = QVBoxLayout()
        layout.addLayout(filter_layout)
        layout.addWidget(self.table)
        self.setLayout(layout)

        day_button.clicked.connect(self.filter_day)
        week_button.clicked.connect(self.filter_week)
        month_button.clicked.connect(self.filter_month)

        self.table.cellDoubleClicked.connect(self._on_cell_double_clicked)

    def init_table(self) -> None:
        """
        Инициализирует таблицу, на которой будут
        отображаться данные о расходах.
        """
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(
            ["Категория", "Сумма расхода", "Дата", "Комментарий", ""])
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents)

        self.table.setEditTriggers(QAbstractItemView.EditTrigger.DoubleClicked)

    def update_table(self, expenses: List[Dict[str, str]]) -> None:
        """
        Обновляет содержимое таблицы
        на основе данных из репозитория.
        """
        self.table.setRowCount(len(expenses))
        for row, expense in enumerate(expenses):
            category_item = self.table.item(row, 0)
            amount_item = self.table.item(row, 1)
            date_item = self.table.item(row, 2)
            description_item = self.table.item(row, 3)
            delete_button = self.table.cellWidget(row, 4)
            if not all((date_item, description_item, category_item, amount_item, delete_button)):
                # делаю формат даты более знакомым
                date_str = expense["date"]
                day, month, year = date_str.split("-")
                date_formatted = f"{year}/{month}/{day}"

                category_item = QTableWidgetItem(expense["category"])
                amount_item = QTableWidgetItem(str(expense["amount"]))
                date_item = QTableWidgetItem(date_formatted)
                description_item = QTableWidgetItem(expense["description"])

                delete_button = QPushButton("Удалить")
                delete_button.clicked.connect(self.delete_row)

                self.table.setItem(row, 0, category_item)
                self.table.setItem(row, 1, amount_item)
                self.table.setItem(row, 2, date_item)
                self.table.setItem(row, 3, description_item)
                self.table.setCellWidget(row, 4, delete_button)
                self.table.setEditTriggers(QAbstractItemView.EditTrigger.DoubleClicked)

    def _on_cell_double_clicked(self, row: int, column: int) -> None:
        if column == 0:
            item = self.table.item(row, column)
            self.category_cell_double_clicked.emit(row, column, item.text())

    def _update_category_cell(self, row: int, column: int, categories: List[Tuple[int, str]]) -> None:
        try:
            combo_box = QComboBox()
            self.categories = categories
            combo_box.addItems([name for _, name in categories])
            self.table.setCellWidget(row, column, combo_box)

            combo_box.currentIndexChanged.connect(lambda index, row=row, column=column, categories=categories:
                                                  self.category_cell_changed.emit(row, column, categories[index][0]))
            self.table.setCellWidget(row, column, combo_box)
        except Exception as e:
            print(f"Ошибка _update_category_cell: {e}")

    def delete_row(self) -> None:
        """
        Удаляет строку с выбранным расходом из списка расходов
        и обновляет содержимое таблицы.
        """
        delete_button: QPushButton = cast(QPushButton, self.sender())
        index: int = self.table.indexAt(delete_button.pos()).row()
        print(index)
        if index >= 0:
            self.delete_button_clicked.emit(index)

    def filter_day(self) -> None:
        """
        заглушка
        """
        pass

    def filter_week(self) -> None:
        """
        заглушка
        """
        pass

    def filter_month(self) -> None:
        """
        заглушка
        """
        pass
