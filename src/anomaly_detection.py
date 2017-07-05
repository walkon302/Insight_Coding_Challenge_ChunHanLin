import json
import os
import sys

def read_json(file):
    """
    TODO:
	Read data from two files, one stores D and T values, and the other stores
    all	events.

    Parameters
    ----------
    file: str
        The path of the json file.

    Returns
    -------
    D: int
        Degree of social network.
    T: int
        Number of purchases.
    log_dict: dict
        key: An integer for the index of events.
        Value: A dictionary for events.
    """
    with open(file) as jf:
        log_dict = {}
        D, T = 0, 0
        for index, case in enumerate(jf.read().splitlines()):
            # Skip if this line is empty
            if case == '':
                continue
            data = json.loads(case)

            # Store the first line of D and T value
            if 'D' in data.keys():
                D, T = data['D'], data['T']
            else:
                log_dict[index] = data
        return int(D), int(T), log_dict

class Person(object):
    """
    A Class, Person.
    Define a person, containing ID, friends, and the history of purchase.

    Attributes
    ----------
    ID: str
        Person's id.
    friend: set
        A set of strings of ids of Person's direct friends.
    purchase: list
        A list of Person's hitory of purchases.
    """
    def __init__(self, ID):
        self.ID = ID
        self.friend = set()
        self.purchase = []

    def add_friend(self, befriend_event):
        """
        Add a new friend to this Person.

        Parameters
        ----------
        befriend_event: dict
            key: A string of 'event_type', 'timestamp', 'id1', and 'id2'.
            value: A string.
        """
        if befriend_event['id1'] == self.ID:
            self.friend.add(befriend_event['id2'])
        else:
            self.friend.add(befriend_event['id1'])

    def delete_friend(self, unfriend_event):
        """
        Delete an exsiting friend. If a friendship doesn't exist, do nothing.

        Parameters
        ----------
        unfriend_event: dict
            key: A string of 'event_type', 'timestamp', 'id1', and 'id2'.
            value: A string.
        """
        to_delete = 0
        if befriend_event['id1'] == self.ID:
            to_delete = unfriend_event['id2']
        else:
            to_delete = unfriend_event['id1']

        try:
            del self.friend.remove[to_delete]
        except:
            pass

    def add_purchase(self, purchase_event, index):
        """
        Add a new purchase history.

        Parameters
        ----------
        purchase_event: dict
            key: A string of 'event_type', 'timestamp', 'id', and 'amount'.
            value: A string.
        index: int
            The index of purchase event in the data. The lower the earlier.
        """
        self.purchase.append(Purchase(purchase_event['amount'],
                                      purchase_event['timestamp'], index))

class Purchase(object):
    """
    A class, Purchase.

    Attributes
    ----------
    amount: float
        The amount of money spent of this purchase.
    timestamp: str
        The time of this purchase event. The smaller the earlier.
    index: int
        The index of purchase event in the data. The lower the earlier.
    """
    def __init__(self, amount, timestamp, index):
        self.amount = float(amount)
        self.timestamp = timestamp
        self.index = index

def std(T_purchase, mean):
    """
    Calculate the standard deviation for T_purchase.

    Parameters
    ----------
    T_purchase: list
        A list of Purchase.
    mean: float
        The mean of amount of purchases.

    Returns
    -------
    std: float
        The standard deviation for purchaese.
    """
    t = 0
    N = len(T_purchase)
    for purchase in T_purchase:
        t += (purchase.amount - mean) ** 2
    t_N = t / N
    std = t_N ** 0.5
    return std

def statistic_calculation(total_network, T, people_list):
    """
    Processor and calculator for purchase events.

    Collect all the purchase events of friends in total_network, sort them by
    the index of events, take at most T numbers of the sorted latest events, and
    calculate mean and standard deviation.

    Parameters
    ----------
    total_network: set
        The set of ids of friends.
    T: int
        The number of purchases that we want to track.
    people_list: dict
        key: A string of Person's ID
        value: A Person.

    Returns
    -------
    total_purchase: list
        A list of Purchase.
    mean_amount: float
        The mean of at most T latest purchases.
    std_amount: float
        The standard deviation of at most T latest purchases.
    """
    total_purchase = []
    total_amount = 0
    mean_amount = 0

    # Collect all events of purchases within person's social network
    for p in [people_list[person_ID] for person_ID in total_network]:
        total_purchase.extend(p.purchase)

    # Only take latest T numbers of purchases.
    total_purchase.sort(key=lambda x: x.index, reverse=True)
    T_purchase = total_purchase[:T]

    # Calculate the total amount of money of purchases
    for purchase in T_purchase:
        total_amount += purchase.amount

    # Calculate the mean and standard deviation of purchases.
    mean_amount = total_amount/len(T_purchase)
    std_amount = std(T_purchase, mean_amount)
    return total_purchase, mean_amount, std_amount

def detect_anomaly(people_list, purchase_event, total_network, T):
    """
    Detect purchase that is 3 standard deviations higher than the avarage of
    within network.

    Parameters
    ----------
    people_list: dict
        key: A string for Person's ID
        value: A Person.
    purchase_event: dict
        key: A string of 'event_type', 'timestamp', 'id', and 'amount'.
        value: A string.
    total_network: set
        The set of ids of friends within network.
    T: int
        The numbers of purchases that we want to track.

    Returns
    -------
    anomaly: str
        A string for the flagged anomaly purchase.
    """
    anomaly = {}
    total_purchase, mean_amount, std_amount = statistic_calculation(
        total_network, T, people_list)
    pattern = '{{"event_type":"purchase", "timestamp":"{}", "id": "{}", "amount": "{}", "mean": "{:.2f}", "sd": "{:.2f}"}}'

    # Skip anomaly of purchases detection if the total number of purchases
    # within social network is less than 2.
    if len(total_purchase) < 2:
        return anomaly
    if float(purchase_event['amount']) > (mean_amount + 3 * std_amount):
        anomaly = pattern.format(purchase_event['timestamp'],
                                 purchase_event['id'],
                                 purchase_event['amount'],
                                 mean_amount,
                                 std_amount)
    return anomaly

def build_history(data):
    """
    Build the people_list, which include the information of Person's friends
    and history of purchases based on initial batch_log file.

    Parameters
    ----------
    data: dict
        key: An integer for the index of events.
        value: An event.

    Returns
    -------
    people_list: dict
        key: A string for Person's ID
        value: A Person.
    """
    people_list = {}
    last_order = 0
    # Iterate through the data.
    for i in data:
        last_order = i
        if data[i]['event_type'] == 'purchase':
            if not data[i]['id'] in people_list.keys():
                people_list[data[i]['id']] = Person(data[i]['id'])
            people_list[data[i]['id']].add_purchase(data[i], i)
        elif data[i]['event_type'] == 'befriend':
            if not data[i]['id1'] in people_list.keys():
                people_list[data[i]['id1']] = Person(data[i]['id1'])
            people_list[data[i]['id1']].add_friend(data[i])
            if not data[i]['id2'] in people_list.keys():
                people_list[data[i]['id2']] = Person(data[i]['id2'])
            people_list[data[i]['id2']].add_friend(data[i])
        else:  # for unfriend events
            # In case of that there are missing events of befriend.
            try:
                people_list[data[i]['id1']].delete_friend(data[i])
                people_list[data[i]['id2']].delete_friend(data[i])
            except:
                pass
    return people_list, last_order

def browse_data(people_list, data, D, T, initial_order):
    """
    Stream the upcoming new data and identify anomaly of purchases within D
    degree of social network, and at most T latest purchases.

    Parameters
    ----------
    people_list: dict
        key: A string for Person's ID
        value: A Person.
    data: dict
        key: An integer for the index of events.
        value: An event.
    D: int
        The number of degree in social network.
    T: int
        The number of purchases that we want to track.

    Returns
    -------
    people_list: dict
        key: A string for Person's ID
        value: A Person.
    anomaly_list: list
        The list of strings of flagged anomaly of purchases.
    """
    assert D >=1, 'Please enter value >= 1 for D'
    assert T >= 2, 'Please enter value >= 2 for T'
    anomaly_list = []

    for i in data:
        curr = i + initial_order
        if data[i]['event_type'] == 'purchase':
            if data[i]['id'] in people_list.keys():
                total_network = friend_network(
                    people_list[data[i]['id']], people_list, D)

                # Skip the anomaly of purchase detection if the person has no
                # friends.
                if len(total_network) >= 1:
                    anomaly = detect_anomaly(
                        people_list, data[i], total_network, T)
                    if anomaly:
                        anomaly_list.append(anomaly)
            else:
                people_list[data[i]['id']] = Person(data[i]['id'])
            people_list[data[i]['id']].add_purchase(data[i], curr)
        elif data[i]['event_type'] == 'befriend':
            if not data[i]['id1'] in people_list.keys():
                people_list[data[i]['id1']] = Person(data[i]['id1'])
            people_list[data[i]['id1']].add_friend(data[i])
            if not data[i]['id2'] in people_list.keys():
                people_list[data[i]['id2']] = Person(data[i]['id2'])
            people_list[data[i]['id2']].add_friend(data[i])
        else:
            try:
                people_list[data[i]['id1']].delete_friend(data[i])
                people_list[data[i]['id2']].delete_friend(data[i])
            except:
                pass
    return people_list, anomaly_list

def net_friendship(person):
    """
    Obtain the set of person's direct friends' id.

    Parameters
    ----------
    person: A Person.

    Returns
    -------
    network: set
        The set of strings of ids of person's direct friends
    """
    network = []

    # Iterate the list of person's friends.
    for person_ID in (person.friend):
        network.append(person_ID)
    return set(network)

def expanding_network(network, people_list):
    """
    Obtain the set of person's friends of friends' id.

    Parameters
    ----------
    network: set
        The set of strings of person's direct friends' id.
    peopls_list: dict
        key: A string for Person's ID
        value: A Person.

    Returns
    -------
    whole_network: set
        The set of person's friends of friends'id.
    """
    whole_network = set()

    # Iterate the list of person's friends of friends.
    for person_ID in network:
        network_new = net_friendship(people_list[person_ID])
        whole_network.update(network_new)
    return whole_network

def friend_network(person, people_list, D):
    """
    Obtain the set of person's friends within D degree of social networks.

    Parameters
    ----------
    person: A Person
    people_list: dict
        key: A string for Person's ID.
        value: A Person.
    D: int
        The number of degree of social network

    Returns
    -------
    network: set
        The set of person's friends within D degree of social networks.
    """
    # Get the list of person's friends.
    network = net_friendship(person)

    # Iterate the social network within D degree.
    while D > 1:
        network = expanding_network(network, people_list)
        D -= 1
    return network

def main():
    input_batch_log = sys.argv[1]
    input_stream_log = sys.argv[2]
    D, T, test_data = read_json(input_batch_log)
    _, _, test_update = read_json(input_stream_log)
    people_list, last_order = build_history(test_data)

    # The first arg is people_list, which can be used for further purposes,
    # for example, we want to know how many friends and how many purchases
    # certain person has.
    _, anomaly_list = browse_data(
        people_list, test_update, D, T, last_order + 1)

    str = '\n'.join(anomaly_list)
    with open(sys.argv[3], 'w') as result:
        result.write(str)

if __name__ == '__main__':
    main()
