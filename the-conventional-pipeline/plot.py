import a2d
import matplotlib.pyplot as plt
from matplotlib import animation

DATABASE_NAME = 'thedb.db'
DB_DIRECTORY = r'c:\Users\User\Desktop\Archive\the-conventional-pipeline\{}'.format(DATABASE_NAME)

# data = a2d.queryt_(db_directory=DB_DIRECTORY, table_name='weather')
# print(data)

def plot(i):
    """Plot data

    Args:
        i (int): An iterator from animation plot
    """
    data = a2d.queryt_(db_directory=DB_DIRECTORY, table_name='weather')
    data = data[data['region'] == 'Kuala Lumpur']
    x = data['request_dt'].values
    y1 = data['temperature'].values
    y2 = data['feelslike'].values
    
    plt.cla()
    plt.plot(x, y1, label='Temperature')
    plt.plot(x, y2, label='Feelslike')
    plt.legend(loc='upper right')
    plt.xlabel('Datetime')
    plt.ylabel('Celcius (℃)')
    plt.title('Current Temperature')

live_plot = animation.FuncAnimation(plt.figure(dpi=100), plot, interval=60000)
plt.show()