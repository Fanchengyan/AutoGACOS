import poplib
import re
from email.message import Message
from email.parser import Parser
from email.utils import parseaddr
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
from tqdm.auto import tqdm


class GACOSEmail:
    """a class to retrieve gacos urls from email.

    .. note::
        The POP3 server is used to retrieve content from email. Some email
        service providers may need to enable the POP3 service in the settings.
        **You are recommended to use a new email account to receive gacos urls to
        avoid polluting your own email account**.
    """

    def __init__(
        self,
        email,
        password,
        popHost,
        port=None,
        gacos_email="gacos2017@foxmail.com",
        url_protocol="http",
        start_date=None,
        end_date=None,
        date_args=None,
        ssl=False,
    ) -> None:
        """Retrieve gacos urls from email.

        Parameters
        ----------
        email : str
            The email address.
        password : str
            The password.
        popHost : str
            The pop host.
        port : int, optional
            The port of the pop host of your email. Default is None.
        gacos_email : str, optional
            The email address of gacos. Default is "gacos2017@foxmail.com".
        url_protocol : str, optional
            The protocol of the gacos url. Default is "http".
        start_date / end_date: str, optional
            The start/end date of email. Used to filter the email. Default is None. Can be any format that can be parsed by pandas.to_datetime.
        date_args : dict, optional
            The arguments are passed to pandas.to_datetime. Default is None.
        ssl : bool, optional
            Whether to use SSL connection. Default is False.
        """
        self.email = email
        self.popHost = popHost
        self.password = password
        self.port = port
        self.gacos_email = gacos_email
        self.url_protocol = url_protocol
        self.ssl = ssl

        # parse date
        if date_args is None:
            date_args = {}

        self.start_date = None
        self.end_date = None
        if start_date is not None:
            self.start_date = pd.Timestamp(
                pd.to_datetime(start_date, **date_args).date()
            )
        if end_date is not None:
            self.end_date = pd.Timestamp(pd.to_datetime(end_date, **date_args).date())

        if (
            (self.start_date is not None)
            and (self.end_date is not None)
            and (self.start_date > self.end_date)
        ):
            raise ValueError(
                f"start_date {self.start_date} is larger than end_date {self.end_date}"
                "Please check your start_date and end_date."
            )

    def retrieve_gacos_urls(
        self,
        output_file: Union[str, Path],
    ):
        """Retrieve gacos urls from email.

        Parameters
        ----------
        output_file : str or Path
            The output file used to save the gacos urls.
        """
        if self.ssl:
            server = login_in_email_ssl(
                self.email, self.password, self.popHost, self.port
            )
        else:
            server = login_in_email(self.email, self.password, self.popHost, self.port)
        print(server.getwelcome())

        nums = server.stat()[0]

        gacos = []
        for i in tqdm(range(1, nums + 1), unit=" emails", desc="Retrieving GACOS Urls"):
            response, msgLines, octets = server.retr(i)
            msgLinesToStr = b"\r\n".join(msgLines).decode("utf8", "ignore")
            messageObject = Parser().parsestr(msgLinesToStr)

            senderContent = messageObject["From"]
            senderRealName, senderAdr = parseaddr(senderContent)
            if senderAdr == self.gacos_email:
                if not in_date_range(
                    pd.Timestamp(pd.to_datetime(messageObject["Date"]).date()),
                    self.start_date,
                    self.end_date,
                ):
                    continue

                msgBodyContents = get_content(messageObject)
                info = parse_gacos_info(msgBodyContents, url_protocol=self.url_protocol)
                if info is not None:
                    gacos.append(info)

        server.quit()

        cols = ["url", "south", "north", "west", "east", "time"]
        df_gacos_urls = pd.DataFrame(gacos, columns=cols)

        # remove duplicated urls
        dupl = df_gacos_urls.duplicated(subset="url")
        df_gacos_urls = df_gacos_urls[~dupl]
        format_gacos_info(df_gacos_urls)

        # save to file
        df_gacos_urls.to_csv(output_file)


def in_date_range(date, start_date, end_date):
    if start_date is None and end_date is None:
        return True
    elif start_date is None:
        return date <= end_date
    elif end_date is None:
        return date >= start_date
    else:
        return (date >= start_date) and (date <= end_date)


def decodeBody(msgPart: Message):
    """decode email body

    Parameters
    ----------
    msgPart : email.message.Message
        The email message object.
    """
    contentType = msgPart.get_content_type()
    textContent = ""
    if contentType == "text/plain" or contentType == "text/html":
        content = msgPart.get_payload(decode=True)
        charset = msgPart.get_charset()
        if charset is None:
            contentType = msgPart.get("Content-Type", "").lower()
            position = contentType.find("charset=")
            if position >= 0:
                charset = contentType[position + 8 :].strip()
        if charset:
            textContent = content.decode(charset)
    return textContent


def get_content(messageObject):
    msgBodyContents = []
    if messageObject.is_multipart():  # parse multipart email
        messageParts = messageObject.get_payload()
        for messagePart in messageParts:
            bodyContent = decodeBody(messagePart)
            if bodyContent:
                msgBodyContents.append(bodyContent)
    else:
        bodyContent = decodeBody(messageObject)
        if bodyContent:
            msgBodyContents.append(bodyContent)
    return msgBodyContents


def login_in_email(email, password, popHost, port):
    try:
        if port is None:
            port = 110
        server = poplib.POP3(popHost, port)
        server.user(email)
        server.pass_(password)
        return server
    except Exception as e:
        print(e)
        print("login failed")


def login_in_email_ssl(email, password, popHost, port):
    try:
        if port is None:
            port = 995
        server = poplib.POP3_SSL(popHost, port)
        server.user(email)
        server.pass_(password)
        return server
    except Exception as e:
        print(e)
        print("login failed")


def parse_gacos_info(msgBodyContents, url_protocol="http"):
    """Parse gacos info from email body.

    Parameters
    ----------
    msgBodyContents : list
        The email body contents.
    url_protocol : str, optional
        The protocol of the url. Default is "http".
    """

    url, south, north, west, east, date = None, None, None, None, None, None
    for contents in msgBodyContents:
        lines = [i.strip() for i in contents.split("\n") if i]
        for line in lines:
            loc = line.split("=")
            if len(loc) == 2:
                parameter, value = loc
                parameter, value = (parameter.strip(), value.strip())
                if "MinLat" == parameter:
                    south = float(value)
                if "MaxLat" == parameter:
                    north = float(value)
                if "MinLon" == parameter:
                    west = float(value)
                if "MaxLon" == parameter:
                    east = float(value)
            loc = line.split(":")
            if len(loc) == 2:
                parameter, value = loc
                parameter, value = (parameter.strip(), value.strip())
                if "Time" == parameter:
                    date = float(value)

            result = re.search(f"\({url_protocol}.*\)", line)
            if result:
                url = result.group()[1:-1]
    if url == south == north == west == east == date:
        return None
    else:
        return url, south, north, west, east, date


def format_gacos_info(df):
    df["south"] = np.round(df["south"], 3)
    df["north"] = np.round(df["north"], 3)
    df["east"] = np.round(df["east"], 3)
    df["west"] = np.round(df["west"], 3)
    df["time"] = np.round(df["time"], 1)
