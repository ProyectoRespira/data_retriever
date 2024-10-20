import telebot
from mage_ai.data_preparation.shared.secrets import get_secret_value
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

AQI_LEVELS = [
    (0, 50, "Bueno 😁"),  # Good
    (51, 100, "Moderado 🙂"),  # Moderate
    (101, 150, "Insalubre para grupos sensibles 😷"),  # Unhealthy for sensitive groups
    (151, 200, "Insalubre 😶‍🌫️"),  # Unhealthy
    (201, 300, "Muy insalubre 😰"),  # Very Unhealthy
    (301, 500, "Peligroso 💀")  # Hazardous
]

AQI_MESSAGES = [
    (0, 50,"🟢 ¡Hoy es un excelente día para estar al aire libre! 🏃‍♂️🍃"),  # Good
    (51, 100, "🟡 Hoy es un buen día para estar al aire libre para la mayoría de las personas. 👍🏻"),  # Moderate
    (101, 150, "🟠 Las personas sensibles deben prestar atención a síntomas 😷"),  # Unhealthy for sensitive groups
    (151, 200, "🔴 Hoy es un mal día para estar al aire libre.\n Se recomienda el uso de tapabocas para la población general💨😷"),  # Unhealthy
    (201, 300, "🟣 Hoy es un muy mal día para estar al aire libre. \n Se recomienda el uso de tapabocas para la población general🔥😷"),  # Very Unhealthy
    (301, 500, "⚫ Evite la exposición al aire libre. 💀😷\nAnte la aparición de síntomas, consulte a su médico.")  # Hazardous

]

def get_aqi_label(aqi_value):
    """Returns the AQI level label for a given AQI value."""
    for min_val, max_val, label in AQI_LEVELS:
        if min_val <= aqi_value <= max_val:
            return label
    return "Fuera del AQI 💀🤯"  

def get_aqi_message(aqi_value):
    for min_val, max_val, message in AQI_MESSAGES:
        if min_val <= aqi_value <= max_val:
            return message
    return " "

def get_latest_aqi_summary(df):
    avg_value = int(df['forecast_avg'].mean())
    max_value = int(df['forecast_avg'].max())
    min_value = int(df['forecast_avg'].min())
    
    return avg_value, max_value, min_value

def construct_message(avg_aqi, max_aqi, min_aqi):
    avg_label = get_aqi_label(avg_aqi)
    max_label = get_aqi_label(max_aqi)
    min_label = get_aqi_label(min_aqi)
    aqi_message = get_aqi_message(avg_aqi)

    message = (
        f"📊 **Reporte de Calidad del Aire para Gran Asunción - Pronóstico de 12 horas**\n"
        f"🔹 **AQI Promedio:** {avg_aqi} ({avg_label})\n"
        f"🔺 **AQI Máximo:** {max_aqi} ({max_label})\n"
        f"🔻 **AQI Mínimo:** {min_aqi} ({min_label})\n\n"
        f"{aqi_message}\n\n"
        "🔗 Podés acceder al pronóstico actualizado en tu zona ingresando a www.proyectorespira.net"
    )
    return message

def send_message(token, chat_id, msg=''):
    bot = telebot.TeleBot(token)
    return bot.send_message(chat_id=chat_id, text=msg, parse_mode='Markdown')

@custom
def transform_custom(data, *args, **kwargs):
    telegram_token = get_secret_value('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = get_secret_value('TELEGRAM_CHAT_ID')
    aqi_summary = get_latest_aqi_summary(data)
    if aqi_summary:
        avg_aqi, max_aqi, min_aqi = aqi_summary
    message = construct_message(avg_aqi, max_aqi, min_aqi)
    send_message(token=telegram_token, chat_id=telegram_chat_id, msg=message)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
