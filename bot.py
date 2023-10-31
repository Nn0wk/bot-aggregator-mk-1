import logging
from aiogram import Bot, Dispatcher, executor, types
import os
from dotenv import load_dotenv
import json

from user_data_check import if__data_valid_return_dict
from db_aggregator import DatabaseAggregator

HELP_MESSAGE = """
Бот считает суммы выплат в указанном промeжутке, по указанной единицы группировки
Пример входных данных:
{"dt_from":"2022-09-01T00:00:00",
"dt_upto":"2022-12-31T23:59:00",
"group_type":"month"} 
"""

load_dotenv()
API_TOKEN = os.environ.get("TELEGRAMM_BOT_TOKEN")
DATABASE_NAME = "sampleDB"
DATABASE_COLLECTION_NAME = "sample_collection"

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
db = DatabaseAggregator(DATABASE_NAME, DATABASE_COLLECTION_NAME)


@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.reply(f"Привет, {message.from_user.first_name}!")


@dp.message_handler(commands=['help'])
async def help(message: types.Message):
    await message.reply(HELP_MESSAGE)


@dp.message_handler()
async def aggregate(message: types.Message):
    logging.info("I have got a message: \n" + message.text)
    answer = "-+Неправильный запрос!+-\n" + HELP_MESSAGE
    message_dict = if__data_valid_return_dict(message.text)
    if message_dict:
        dt_from = message_dict["dt_from"]
        dt_upto = message_dict["dt_upto"]
        group_type = message_dict["group_type"]
        answer = await db.aggregate(dt_from, dt_upto, group_type)
        #answer = json.dumps(answer, indent=4)
        answer = json.dumps(answer)
    await message.answer(answer)



if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)