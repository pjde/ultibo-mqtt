unit uMQTTServer;

{$mode delphi}{$H+}

interface

uses
  Classes, SysUtils, GlobalTypes, Winsock2, uMQTT, Threads, Platform, SyncObjs;

const
  MinVersion         = 3;

  stInit             = 0;
  stClosed           = 1;
  stConnecting       = 2;
  stConnected        = 3;
  stClosing          = 4;
  stError            = 5;
  stQuitting         = 6;

type
  TMQTTClient = class;
  TMQTTThread = class;
  TMQTTPacketStore = class;
  TMQTTMessageStore = class;
  TMQTTServer = class;

  TTimerRec = record
    Owner : TObject;
    No : LongWord;
    Handle : TTimerHandle;
  end;
  PTimerRec = ^TTimerRec;

  TMQTTPacket = class
    ID : Word;
    Stamp : TDateTime;
    Counter : cardinal;
    Retries : integer;
    Publishing : Boolean;
    Msg : TMemoryStream;
    procedure Assign (From : TMQTTPacket);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTMessage = class
    ID : Word;
    Stamp : TDateTime;
    LastUsed : TDateTime;
    Qos : TMQTTQOSType;
    Retained : boolean;
    Counter : cardinal;
    Retries : integer;
    Topic : UTF8String;
    Message : string;
    procedure Assign (From : TMQTTMessage);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTSession = class
    ClientID : UTF8String;
    Stamp : TDateTime;
    InFlight : TMQTTPacketStore;
    Releasables : TMQTTMessageStore;
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTSessionStore = class
    List :  TList;
    Stamp : TDateTime;
    function GetItem (Index: Integer): TMQTTSession;
    procedure SetItem (Index: Integer; const Value: TMQTTSession);
    property Items [Index: Integer]: TMQTTSession read GetItem write SetItem; default;
    function Count : integer;
    procedure Clear;
    function GetSession (ClientID : UTF8String) : TMQTTSession;
    procedure StoreSession (ClientID : UTF8String; aClient : TMQTTThread); overload;
    procedure StoreSession (ClientID : UTF8String; aClient : TMQTTClient); overload;
    procedure DeleteSession (ClientID : UTF8String);
    procedure RestoreSession (ClientID : UTF8String; aClient : TMQTTThread); overload;
    procedure RestoreSession (ClientID : UTF8String; aClient : TMQTTClient); overload;
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTPacketStore = class
    List : TList;
    Stamp : TDateTime;
    function GetItem (Index: Integer): TMQTTPacket;
    procedure SetItem (Index: Integer; const Value: TMQTTPacket);
    property Items [Index: Integer]: TMQTTPacket read GetItem write SetItem; default;
    function Count : integer;
    procedure Clear;
    procedure Assign (From : TMQTTPacketStore);
    function AddPacket (anID : Word; aMsg : TMemoryStream; aRetry : cardinal; aCount : cardinal) : TMQTTPacket;
    procedure DelPacket (anID : Word);
    function GetPacket (anID : Word) : TMQTTPacket;
    procedure Remove (aPacket : TMQTTPacket);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTMessageStore = class
    List : TList;
    Stamp : TDateTime;
    function GetItem (Index: Integer): TMQTTMessage;
    procedure SetItem (Index: Integer; const Value: TMQTTMessage);
    property Items [Index: Integer]: TMQTTMessage read GetItem write SetItem; default;
    function Count : integer;
    procedure Clear;
    procedure Assign (From : TMQTTMessageStore);
    function AddMsg (anID : Word; aTopic : UTF8String; aMessage : AnsiString; aQos : TMQTTQOSType; aRetry : cardinal; aCount : cardinal; aRetained : Boolean = false) : TMQTTMessage;
    procedure DelMsg (anID : Word);
    function GetMsg (anID : Word) : TMQTTMessage;
    procedure Remove (aMsg : TMQTTMessage);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTThread = class (TWinsock2TCPServerThread)
  private
    FOnMon : TMQTTMonEvent;
    Owner : TMQTTServer;
    FGraceful : boolean;
    FBroker : Boolean;        // non standard
    FOnSubscriptionChange: TNotifyEvent;
    procedure DoSend (Sender : TObject; anID : Word; aRetry : integer; aStream : TMemoryStream);
    procedure RxSubscribe (Sender : TObject; anID : Word; Topics : TStringList);
    procedure RxUnsubscribe (Sender : TObject; anID : Word; Topics : TStringList);
    procedure RxPubAck (Sender : TObject; anID : Word);
    procedure RxPubRec (Sender : TObject; anID : Word);
    procedure RxPubRel (Sender : TObject; anID : Word);
    procedure RxPubComp (Sender : TObject; anID : Word);
  public
    Subscriptions : TStringList;
    Parser : TMQTTParser;
    InFlight : TMQTTPacketStore;
    Releasables : TMQTTMessageStore;
    procedure DoSetWill (Sender : TObject; aTopic, aMessage : UTF8String; aQOS : TMQTTQOSType; aRetain : boolean);
    constructor Create (aServer : TWinsock2TCPServer);
    destructor Destroy; override;
    property OnSubscriptionChange : TNotifyEvent read FOnSubscriptionChange write FOnSubscriptionChange;
    property OnMon : TMQTTMonEvent read FOnMon write FOnMon;
  end;

  { TMQTTClient }

  TMQTTClient = class (TThread)
  private
    Timers : array [1 .. 3] of TTimerRec;
    FUsername, FPassword : UTF8String;
    FMessageID : Word;
    FHost : string;
    FPort : integer;
    FState : integer;
    FEnable, FOnline : Boolean;
    FGraceful : Boolean;
    FOnOnline: TNotifyEvent;
    FOnOffline: TMQTTDisconnectEvent;
    FOnEnableChange: TNotifyEvent;
    FOnMsg: TMQTTMsgEvent;
    FOnFailure: TMQTTFailureEvent;
    FLocalBounce: Boolean;
    FAutoSubscribe: Boolean;
    FOnClientID : TMQTTClientIDEvent;
    FBroker: Boolean;     // non standard
    FEvent : TEvent;
    procedure DoSend (Sender : TObject; anID : Word; aRetry : integer; aStream : TMemoryStream);
    procedure RxConnAck (Sender : TObject; aCode : byte);
    procedure RxSubAck (Sender : TObject; anID : Word; Qoss : array of TMQTTQosType);
    procedure RxPubAck (Sender : TObject; anID : Word);
    procedure RxPubRec (Sender : TObject; anID : Word);
    procedure RxPubRel (Sender : TObject; anID : Word);
    procedure RxPubComp (Sender : TObject; anID : Word);
    procedure RxPublish (Sender : TObject; anID : Word; aTopic : UTF8String; aMessage : string);
    procedure RxUnsubAck (Sender : TObject; anID : Word);
    function GetClientID: UTF8String;
    procedure SetClientID (const Value: UTF8String);
    function GetKeepAlive: Word;
    procedure SetKeepAlive (const Value: Word);
    function GetMaxRetries : integer;
    procedure SetMaxRetries (const Value: integer);
    function GetRetryTime : cardinal;
    procedure SetRetryTime (const Value : cardinal);
    function GetClean: Boolean;
    procedure SetClean (const Value: Boolean);
    function GetPassword: UTF8String;
    function GetUsername: UTF8String;
    procedure SetPassword (const Value: UTF8String);
    procedure SetUsername (const Value: UTF8String);
  protected
    procedure SetState (NewState : integer);
    procedure Execute; override;
  public
    Link : TWinsock2TCPClient;
    Parser : TMQTTParser;
    InFlight : TMQTTPacketStore;
    Releasables : TMQTTMessageStore;
    Subscriptions : TStringList;
    procedure SetTimer (No, Interval : LongWord; Once : boolean);
    procedure KillTimer (No : Longword);
    function Enabled : boolean;
    function Online : boolean;
    function NextMessageID : Word;
    procedure Subscribe (aTopic : UTF8String; aQos : TMQTTQOSType); overload;
    procedure Subscribe (Topics : TStringList); overload;
    procedure Unsubscribe (aTopic : UTF8String); overload;
    procedure Unsubscribe (Topics : TStringList); overload;
    procedure Ping;
    procedure Publish (aTopic : UTF8String; aMessage : string; aQos : TMQTTQOSType; aRetain : Boolean = false);
    procedure SetWill (aTopic, aMessage : UTF8String; aQos : TMQTTQOSType; aRetain : Boolean = false);
    procedure Activate (Enable : Boolean);
    constructor Create;
    destructor Destroy; override;
  published
    property ClientID : UTF8String read GetClientID write SetClientID;
    property KeepAlive : Word read GetKeepAlive write SetKeepAlive;
    property MaxRetries : integer read GetMaxRetries write SetMaxRetries;
    property RetryTime : cardinal read GetRetryTime write SetRetryTime;
    property Clean : Boolean read GetClean write SetClean;
    property Broker : Boolean read FBroker write FBroker;   // no standard
    property AutoSubscribe : Boolean read FAutoSubscribe write FAutoSubscribe;
    property Username : UTF8String read GetUsername write SetUsername;
    property Password : UTF8String read GetPassword write SetPassword;
    property Host : string read FHost write FHost;
    property Port : integer read FPort write FPort;
    property LocalBounce : Boolean read FLocalBounce write FLocalBounce;
    property OnClientID : TMQTTClientIDEvent read FOnClientID write FOnClientID;
    property OnOnline : TNotifyEvent read FOnOnline write FOnOnline;
    property OnOffline : TMQTTDisconnectEvent read FOnOffline write FOnOffline;
    property OnEnableChange : TNotifyEvent read FOnEnableChange write FOnEnableChange;
    property OnFailure : TMQTTFailureEvent read FOnFailure write FOnFailure;
    property OnMsg : TMQTTMsgEvent read FOnMsg write FOnMsg;
  end;

  { TMQTTServer }

  TMQTTServer = class (TWinsock2TCPListener)
  protected
    procedure DoConnect (aThread : TWinsock2TCPServerThread); override;
    procedure DoDisconnect (aThread : TWinsock2TCPServerThread); override;
    function DoExecute (aThread : TWinsock2TCPServerThread) : Boolean; override;
  private
    FOnMon : TMQTTMonEvent;
    FOnClientsChange: TMQTTIDEvent;
    FOnCheckUser: TMQTTCheckUserEvent;
    FPort : integer;
    FEnable : boolean;
    FOnBrokerOffline: TMQTTDisconnectEvent;
    FOnBrokerOnline: TNotifyEvent;
    FOnBrokerEnableChange: TNotifyEvent;
    FOnObituary: TMQTTObituaryEvent;
    FOnEnableChange: TNotifyEvent;
    FLocalBounce: Boolean;
    FOnSubscription: TMQTTSubscriptionEvent;
    FOnFailure: TMQTTFailureEvent;
    FMaxRetries: integer;
    FRetryTime: cardinal;
    FOnStoreSession: TMQTTSessionEvent;
    FOnRestoreSession: TMQTTSessionEvent;
    FOnDeleteSession: TMQTTSessionEvent;

//    FOnRetain: TMQTTRetainEvent;
//    FOnGetRetained: TMQTTRetainedEvent;
    // broker events
    procedure BkrOnline (Sender : TObject);
    procedure BkrOffline (Sender : TObject; Graceful : boolean);
    procedure BkrEnableChanged (Sender : TObject);
    procedure BkrSubscriptionChange (Sender : TObject);
    procedure BkrMsg (Sender : TObject; aTopic : UTF8String; aMessage : AnsiString; aQos : TMQTTQOSType; aRetained : boolean);
    // socket events
    procedure DoCreateThread (aServer : TWinsock2TCPServer; var aThread : TWinsock2TCPServerThread);
 //    procedure DoClientConnect (Sender: TObject; Client: TWSocketClient; Error: Word);
//    procedure DoClientDisconnect (Sender: TObject; Client: TWSocketClient; Error: Word);
//    procedure DoClientCreate (Sender: TObject; Client: TWSocketClient);
    // parser events
    procedure RxDisconnect (Sender : TObject);
    procedure RxPing (Sender : TObject);
    procedure RxPublish (Sender : TObject; anID : Word; aTopic : UTF8String; aMessage : AnsiString);
    procedure RxHeader (Sender : TObject; MsgType: TMQTTMessageType; Dup: Boolean;
                        Qos: TMQTTQOSType; Retain: Boolean);
    procedure RxConnect (Sender : TObject;
                        aProtocol : UTF8String;
                        aVersion : byte;
                        aClientID,
                        aUserName, aPassword : UTF8String;
                        aKeepAlive : Word; aClean : Boolean);
    procedure RxBrokerConnect (Sender : TObject;      // non standard
                        aProtocol : UTF8String;
                        aVersion : byte;
                        aClientID,
                        aUserName, aPassword : UTF8String;
                        aKeepAlive : Word; aClean : Boolean);
    procedure SetMaxRetries (const Value: integer);
    procedure SetRetryTime (const Value: cardinal);
  public
    FOnMonHdr : TMQTTHeaderEvent;
    Timers : array [1 .. 3] of TTimerRec;
    MessageID : Word;
    Brokers : TList;
    Sessions : TMQTTSessionStore;
    Retained : TMQTTMessageStore;
    procedure SetTimer (No, Interval : LongWord; Once : boolean);
    procedure KillTimer (No : Longword);
    function NextMessageID : Word;
    procedure Activate (Enable : boolean);
    procedure LoadBrokers (anIniFile : string);
    procedure StoreBrokers (anIniFile : string);
    function GetClient (aParser : TMQTTParser) : TMQTTThread; overload;
    function GetClient (aClientID : UTF8String) : TMQTTThread; overload;

    procedure PublishToAll (From : TObject; aTopic : UTF8String; aMessage : AnsiString; aQos : TMQTTQOSType; wasRetained : boolean = false);
    function Enabled : boolean;
    function AddBroker (aHost : string; aPort : integer) : TMQTTClient;
    procedure SyncBrokerSubscriptions (aBroker : TMQTTClient);
    constructor Create;
    destructor Destroy; override;
  published
    property MaxRetries : integer read FMaxRetries write SetMaxRetries;
    property RetryTime : cardinal read FRetryTime write SetRetryTime;   // in secs
    property Port : integer read FPort write FPort;
    property LocalBounce : Boolean read FLocalBounce write FLocalBounce;
    property OnFailure : TMQTTFailureEvent read FOnFailure write FOnFailure;
    property OnStoreSession : TMQTTSessionEvent read FOnStoreSession write FOnStoreSession;
    property OnRestoreSession : TMQTTSessionEvent read FOnRestoreSession write FOnRestoreSession;
    property OnDeleteSession : TMQTTSessionEvent read FOnDeleteSession write FOnDeleteSession;
//    property OnRetain : TMQTTRetainEvent read FOnRetain write FOnRetain;
//    property OnGetRetained : TMQTTRetainedEvent read FOnGetRetained write FOnGetRetained;
    property OnBrokerOnline : TNotifyEvent read FOnBrokerOnline write FOnBrokerOnline;
    property OnBrokerOffline : TMQTTDisconnectEvent read FOnBrokerOffline write FOnBrokerOffline;
    property OnBrokerEnableChange : TNotifyEvent read FOnBrokerEnableChange write FOnBrokerEnableChange;
    property OnEnableChange : TNotifyEvent read FOnEnableChange write FOnEnableChange;
    property OnSubscription : TMQTTSubscriptionEvent read FOnSubscription write FOnSubscription;
    property OnClientsChange : TMQTTIDEvent read FOnClientsChange write FOnClientsChange;
    property OnCheckUser : TMQTTCheckUserEvent read FOnCheckUser write FOnCheckUser;
    property OnObituary : TMQTTObituaryEvent read FOnObituary write FOnObituary;
    property OnMon : TMQTTMonEvent read FOnMon write FOnMon;
  end;

function SubTopics (aTopic : UTF8String) : TStringList;
function IsSubscribed (aSubscription, aTopic : UTF8String) : boolean;

implementation

uses
  IniFiles, uLog, GlobalConst;

function StateToStr (s : integer) : string;
begin
  case s of
    stInit       : Result := 'Init';
    stClosed     : Result := 'Closed';
    stConnecting : Result := 'Connecting';
    stConnected  : Result := 'Connected';
    stClosing    : Result := 'Closing';
    stError      : Result := 'Error';
    stQuitting   : Result := 'Quitting';
    else           Result := 'Unknown ' + IntToStr (s);
    end;
end;

function SubTopics (aTopic : UTF8String) : TStringList;
var
  i : integer;
begin
  Result := TStringList.Create;
  Result.Add ('');
  for i := 1 to length (aTopic) do
    begin
      if aTopic[i] = '/' then
        Result.Add ('')
      else
        Result[Result.Count - 1] := Result[Result.Count - 1] + Char (aTopic[i]);
    end;
end;

function IsSubscribed (aSubscription, aTopic : UTF8String) : boolean;
var
  s, t : TStringList;
  i : integer;
  MultiLevel : Boolean;
begin
  s := SubTopics (aSubscription);
  t := SubTopics (aTopic);
  MultiLevel := (s[s.Count - 1] = '#');   // last field is #
  if not MultiLevel then
    Result := (s.Count = t.Count)
  else
    Result := (s.Count <= t.Count + 1);
  if Result then
    begin
      for i := 0 to s.Count - 1 do
        begin
          if (i >= t.Count) then Result := MultiLevel
          else if (i = s.Count - 1) and (s[i] = '#') then break
          else if s[i] = '+' then continue    // they match
          else
            Result := Result and (s[i] = t[i]);
          if not Result then break;
        end;
    end;
  s.Free;
  t.Free;
end;

procedure SetDup (aStream : TMemoryStream; aState : boolean);
var
  x : byte;
begin
  if aStream.Size = 0 then exit;
  aStream.Seek (0, soFromBeginning);
  x := 0;
  aStream.Read (x, 1);
  x := (x and $F7) or (ord (aState) * $08);
  aStream.Seek (0, soFromBeginning);
  aStream.Write (x, 1);
end;

{ TMQTTThread }

constructor TMQTTThread.Create (aServer : TWinsock2TCPServer);
begin
  inherited Create (aServer);
  FBroker := false;       // non standard
  Parser := TMQTTParser.Create;
  Parser.OnSend := DoSend;
  Parser.OnSetWill := DoSetWill;
  Parser.OnSubscribe := RxSubscribe;
  Parser.OnUnsubscribe := RxUnsubscribe;
  Parser.OnPubAck := RxPubAck;
  Parser.OnPubRel := RxPubRel;
  Parser.OnPubRec := RxPubRec;
  Parser.OnPubComp := RxPubComp;
  InFlight := TMQTTPacketStore.Create;
  Releasables := TMQTTMessageStore.Create;
  Subscriptions := TStringList.Create;
end;

destructor TMQTTThread.Destroy;
begin
  InFlight.Clear;
  InFlight.Free;
  Releasables.Clear;
  Releasables.Free;
  Parser.Free;
  Subscriptions.Free;
  inherited;
end;

procedure TMQTTThread.DoSend (Sender: TObject; anID : Word; aRetry : integer; aStream: TMemoryStream);
var
  x : byte;
begin
 // if FState = stConnected then
 //   begin
      x := 0;
      aStream.Seek (0, soFromBeginning);
      aStream.Read (x, 1);
      if (TMQTTQOSType ((x and $06) shr 1) in [qtAT_LEAST_ONCE, qtEXACTLY_ONCE]) and
         (TMQTTMessageType ((x and $f0) shr 4) in [{mtPUBREL,} mtPUBLISH, mtSUBSCRIBE, mtUNSUBSCRIBE]) and
         (anID > 0) then
        begin
          InFlight.AddPacket (anID, aStream, aRetry, Parser.RetryTime);      // start disabled
          Log (Parser.ClientID + ' Message ' + IntToStr (anID) + ' created.');
        end;
      Server.WriteData (aStream.Memory, aStream.Size);
 //   end;
end;

procedure TMQTTThread.DoSetWill (Sender: TObject; aTopic, aMessage: UTF8String;
  aQos : TMQTTQOSType; aRetain: boolean);
begin
  Parser.WillTopic := aTopic;
  Parser.WillMessage := aMessage;
  Parser.WillQos := aQos;
  Parser.WillRetain := aRetain;
end;

procedure TMQTTThread.RxPubAck (Sender: TObject; anID: Word);
begin
  InFlight.DelPacket (anID);
  Log (Parser.ClientID + ' ACK Message ' + IntToStr (anID) + ' disposed of.');
end;

procedure TMQTTThread.RxPubComp (Sender: TObject; anID: Word);
begin
  InFlight.DelPacket (anID);
  Log (Parser.ClientID + ' COMP Message ' + IntToStr (anID) + ' disposed of.');
end;

procedure TMQTTThread.RxPubRec (Sender: TObject; anID: Word);
var
  aPacket : TMQTTPacket;
begin
  aPacket := InFlight.GetPacket (anID);
  if aPacket <> nil then
    begin
      aPacket.Counter := Parser.RetryTime;
      if aPacket.Publishing then
        begin
          aPacket.Publishing := false;
          Log (Parser.ClientID + ' REC Message ' + IntToStr (anID) + ' recorded.');
        end
      else
        Log (Parser.ClientID + ' REC Message ' + IntToStr (anID) + ' already recorded.');
    end
  else
    Log (Parser.ClientID + ' REC Message ' + IntToStr (anID) + ' not found.');
  Parser.SendPubRel (anID);
end;

procedure TMQTTThread.RxPubRel (Sender: TObject; anID: Word);
var
  aMsg : TMQTTMessage;
begin
  aMsg := Releasables.GetMsg (anID);
  if aMsg <> nil then
    begin
      Log (Parser.ClientID + ' REL Message ' + IntToStr (anID) + ' publishing @ ' + QOSNames[aMsg.Qos]);
      Owner.PublishToAll (Self, aMsg.Topic, aMsg.Message, aMsg.Qos);
      Releasables.Remove (aMsg);
      aMsg.Free;
      Log (Parser.ClientID + ' REL Message ' + IntToStr (anID) + ' removed from storage.');
    end
  else
    Log (Parser.ClientID + ' REL Message ' + IntToStr (anID) + ' has been already removed from storage.');
  Parser.SendPubComp (anID);
end;

procedure TMQTTThread.RxSubscribe (Sender: TObject; anID: Word; Topics: TStringList);
var
  x : cardinal;
  q : TMQTTQOSType;
  i, j : integer;
  found : boolean;
  Qoss : array of TMQTTQOSType;
  bMsg : TMQTTMessage;
  aQos : TMQTTQOSType;
begin
  SetLength (Qoss, Topics.Count);
  for i := 0 to Topics.Count - 1 do
    begin
      found := false;
      x := cardinal (Topics.Objects[i]) and $03;
      q := TMQTTQOSType (x);
      if Assigned (Owner.FOnSubscription) then
        Owner.FOnSubscription (Self, UTF8String (Topics[i]), q);
      for j := 0 to Subscriptions.Count - 1 do
        if Subscriptions[j] = Topics[i] then
          begin
            found := true;
            Subscriptions.Objects[j] := TObject (q);
            break;
          end;
      if not found then
        begin
          Subscriptions.AddObject (Topics[i], TObject (q));
        end;
      Qoss[i] := q;
      for j := 0 to Owner.Retained.Count - 1 do     // set retained
        begin
          bMsg := Owner.Retained[j];
          if IsSubscribed (UTF8String (Topics[i]), bMsg.Topic) then
            begin
              aQos := bMsg.Qos;
              if q < aQos then aQos := q;
              bMsg.LastUsed := Now;
              Parser.SendPublish (Owner.NextMessageID, bMsg.Topic, bMsg.Message, aQos, false, true);
            end;
        end;
    end;
  if Parser.RxQos = qtAT_LEAST_ONCE then Parser.SendSubAck (anID, Qoss);
  if Assigned (FOnSubscriptionChange) then FOnSubscriptionChange (Self);
end;

procedure TMQTTThread.RxUnsubscribe (Sender: TObject; anID: Word; Topics: TStringList);
var
  i, j : integer;
  changed : boolean;
begin
  changed := false;
  for i := 0 to Topics.Count - 1 do
    begin
      for j := Subscriptions.Count - 1 downto 0 do
        begin
          if Subscriptions[j] = Topics[i] then
            begin
              Subscriptions.Delete (j);
              changed := true;
            end;
        end;
    end;
  if changed and  Assigned (FOnSubscriptionChange) then
    FOnSubscriptionChange (Self);
  if Parser.RxQos = qtAT_LEAST_ONCE then Parser.SendUnSubAck (anID);
end;

{ TMQTTServer }

procedure TMQTTServer.Activate (Enable: boolean);
begin
  if FEnable = Enable then exit;
  if (Enable) then
    begin
      BoundPort := FPort;
      try
        Active := true;
        FEnable := true;
      except
        FEnable := false;
        end;
      if FEnable then SetTimer (3, 100, true);
    end
  else
    begin
      FEnable := false;
      Active := false;
      KillTimer (1);
      KillTimer (2);
      KillTimer (3);
    end;
  if Assigned (FOnEnableChange) then FOnEnableChange (Self);
end;

function TMQTTServer.AddBroker (aHost: string; aPort: integer): TMQTTClient;
begin
  Result := TMQTTClient.Create;
  Result.Host := aHost;
  Result.Port := aPort;
  Result.Broker := true;
  Result.LocalBounce := false;
  Result.OnOnline := BkrOnline;
  Result.OnOffline := BkrOffline;
  Result.OnEnableChange := BkrEnableChanged;
  Result.OnMsg := BkrMsg;
  Brokers.Add (Result);
end;

procedure TMQTTServer.BkrEnableChanged (Sender: TObject);
begin
  if Assigned (FOnBrokerEnableChange) then FOnBrokerEnableChange (Sender);
end;

procedure TMQTTServer.BkrOffline (Sender: TObject; Graceful: boolean);
begin
  TMQTTClient (Sender).Subscriptions.Clear;
  if Assigned (FOnBrokerOffline) then FOnBrokerOffline (Sender, Graceful);
end;

procedure TMQTTServer.BkrOnline (Sender: TObject);
begin
  SyncBrokerSubscriptions (TMQTTClient (Sender));
  if Assigned (FOnBrokerOnline) then FOnBrokerOnline (Sender);
end;

procedure TMQTTServer.BkrMsg (Sender: TObject; aTopic : UTF8String; aMessage : AnsiString; aQos : TMQTTQOSType; aRetained : boolean);
var
  aBroker : TMQTTClient;
  i : integer;
  aMsg : TMQTTMessage;
begin
  aBroker := TMQTTClient (Sender);
  Log ('Received Retained Message from a Broker - Retained ' + ny[aRetained]);
  if aRetained then
    begin
      Log ('Retaining "' + string (aTopic) + '" @ ' + QOSNames[aQos]);
      for i := Retained.Count - 1 downto 0 do
        begin
          aMsg := Retained[i];
          if aMsg.Topic = aTopic then
            begin
              Retained.Remove (aMsg);
              aMsg.Free;
              break;
            end;
        end;
      Retained.AddMsg (0, aTopic, aMessage, aQos, 0, 0);
    end
  else
    Log ('Received Message from a Broker - Publishing..');
  PublishToAll (Sender, aTopic, aMessage, aBroker.Parser.RxQos, aRetained);
end;

procedure TMQTTServer.DoCreateThread (aServer: TWinsock2TCPServer;
  var aThread: TWinsock2TCPServerThread);
begin
  Log ('Thread Created');
  aThread := TMQTTThread.Create (aServer);
  with TMQTTThread (aThread) do
    begin
      Owner := Self;
      Parser.OnPing := RxPing;
      Parser.OnDisconnect := RxDisconnect;
      Parser.OnPublish := RxPublish;
      Parser.OnPubRec := RxPubRec;
      Parser.OnConnect := RxConnect;
      Parser.OnBrokerConnect := RxBrokerConnect;    // non standard
      Parser.OnHeader := RxHeader;
      Parser.MaxRetries := FMaxRetries;
      Parser.RetryTime := FRetryTime;
      OnSubscriptionChange := BkrSubscriptionChange;
  end;
end;

procedure TMQTTServer.DoConnect (aThread: TWinsock2TCPServerThread);
begin
  inherited DoConnect (aThread);
  Log ('Connect');
//  if Assigned (
end;

procedure TMQTTServer.DoDisconnect (aThread: TWinsock2TCPServerThread);
var
  aTopic, aMessage : UTF8String;
  aQos : TMQTTQOSType;
  aClient : TMQTTThread;
begin
  aClient := TMQTTThread (aThread);
  with aClient do
    begin
      Log ('Client Disconnected.  Graceful ' + ny[FGraceful]);
      if (InFlight.Count > 0) or (Releasables.Count > 0) then
        begin
          if Assigned (FOnStoreSession) then
            FOnStoreSession (aClient, Parser.ClientID)
          else
            Sessions.StoreSession (Parser.ClientID, aClient);
        end;
      if not FGraceful then
        begin
          aTopic := Parser.WillTopic;
          aMessage := Parser.WillMessage;
          aQos := Parser.WillQos;
          if Assigned (FOnObituary) then FOnObituary (aClient, aTopic, aMessage, aQos);
          PublishToAll (nil, aTopic, aMessage, aQos);
        end;
    end;
  if Assigned (FOnClientsChange) then FOnClientsChange (Self, Threads.Count - 1);
  inherited DoDisconnect (aThread);
end;

function TMQTTServer.DoExecute (aThread: TWinsock2TCPServerThread): Boolean;
var
  aClient : TMQTTThread;
  d : boolean;
  b : array [0..255] of byte;
  c : integer;
  closed : boolean;
begin
  Result := inherited DoExecute (aThread);
  aClient := TMQTTThread (aThread);
  c := 256;
  closed := false;
  d := aThread.Server.ReadAvailable (@b[0], 255, c, closed);
  if closed or not d then Result := false;
  if (c = 0) or closed then exit;
  aClient.Parser.Parse (@b[0], c);
end;

procedure TMQTTServer.BkrSubscriptionChange (Sender: TObject);
var
  i : integer;
begin
  Log ('Subscriptions changed...');
  for i := 0 to Brokers.Count - 1 do
    SyncBrokerSubscriptions (TMQTTClient (Brokers[i]));
end;

constructor TMQTTServer.Create;
var
  i : integer;
begin
  inherited;
  for i := 1 to 3 do
    begin
      Timers[i].Handle := INVALID_HANDLE_VALUE;
      Timers[i].No := i;
      Timers[i].Owner := Self;
    end;
  MessageID := 1000;
  FOnMonHdr := nil;
  FPort := 1883;
  FMaxRetries := DefMaxRetries;
  FRetryTime := DefRetryTime;
  Brokers := TList.Create;
  Sessions := TMQTTSessionStore.Create;
  Retained := TMQTTMessageStore.Create;
  OnCreateThread := DoCreateThread;
end;

destructor TMQTTServer.Destroy;
var
  i : integer;
begin
  for i := 1 to 3 do KillTimer (i);
  for i := 0 to Brokers.Count - 1 do TMQTTClient (Brokers[i]).Free;
  Brokers.Free;
  Retained.Free;
  Sessions.Free;
  Activate (false);
  inherited;
end;

procedure TMQTTServer.RxPing (Sender: TObject);
begin
  if not (Sender is TMQTTParser) then exit;
  TMQTTParser (Sender).SendPingResp;
end;

procedure TMQTTServer.RxPublish (Sender: TObject; anID: Word; aTopic : UTF8String;
  aMessage: AnsiString);
var
  aParser : TMQTTParser;
  aClient : TMQTTThread;
  aMsg : TMQTTMessage;
  i : integer;
begin
  if not (Sender is TMQTTParser) then exit;
  aParser := TMQTTParser (Sender);
  aClient := GetClient (aParser);
  if aClient = nil then exit;
  if aParser.RxRetain then
    begin
      Log ('Retaining "' + string (aTopic) + '" @ ' + QOSNames[aParser.RxQos]);
      for i := Retained.Count - 1 downto 0 do
        begin
          aMsg := Retained[i];
          if aMsg.Topic = aTopic then
            begin
              Retained.Remove (aMsg);
              aMsg.Free;
              break;
            end;
        end;
      Retained.AddMsg (0, aTopic, aMessage, aParser.RxQos, 0, 0);
    end;
  case aParser.RxQos of
    qtAT_MOST_ONCE  :
      PublishToAll (aClient, aTopic, aMessage, aParser.RxQos, aParser.RxRetain);
    qtAT_LEAST_ONCE :
      begin
        aParser.SendPubAck (anID);
        PublishToAll (aClient, aTopic, aMessage, aParser.RxQos, aParser.RxRetain);
      end;
    qtEXACTLY_ONCE  :
      begin
        aMsg := aClient.Releasables.GetMsg (anID);
        if aMsg = nil then
          begin
            aClient.Releasables.AddMsg (anID, aTopic, aMessage, aParser.RxQos, 0, 0);
            Log (aClient.Parser.ClientID + ' Message ' + IntToStr (anID) + ' stored and idle.');
          end
        else
          Log (aClient.Parser.ClientID + ' Message ' + IntToStr (anID) + ' already stored.');
        aParser.SendPubRec (anID);
      end;
  end;
end;

procedure TMQTTServer.LoadBrokers (anIniFile: string);
var
  i : integer;
  Sections : TStringList;
  aBroker : TMQTTClient;
  EnFlag : Boolean;
begin
  for i := 0 to Brokers.Count - 1 do TMQTTClient (Brokers[i]).Free;
  Brokers.Clear;
  Sections := TStringList.Create;
  with TIniFile.Create (anIniFile) do
    begin
      ReadSections (Sections);
      for i := 0 to Sections.Count - 1 do
        begin
          if Copy (Sections[i], 1, 6) = 'BROKER' then
            begin
              aBroker := AddBroker ('', 0);
              aBroker.Host := ReadString (Sections[i], 'Prim Host', '');
              aBroker.Port := ReadInteger (Sections[i], 'Port', 1883);
              EnFlag := ReadBool (Sections[i], 'Enabled', false);
              if EnFlag then aBroker.Activate (true);
            end;
        end;
      Free;
    end;
  Sections.Free;
end;

procedure TMQTTServer.SetMaxRetries (const Value: integer);
var
  i : integer;
  aClient : TMQTTThread;
begin
  FMaxRetries := Value;
  aClient := TMQTTThread (Threads.First);
  while aClient <> nil do
    begin
      aClient.Parser.MaxRetries := Value;
      aClient := TMQTTThread (aClient.Next);
    end;
 for i := 0 to Brokers.Count - 1 do
   TMQTTClient (Brokers[i]).Parser.MaxRetries := Value;
end;

procedure TMQTTServer.SetRetryTime (const Value: cardinal);
var
  i : integer;
  aClient : TMQTTThread;
begin
  FRetryTime := Value;
    aClient := TMQTTThread (Threads.First);
  while aClient <> nil do
    begin
      aClient.Parser.KeepAlive := Value;
      aClient := TMQTTThread (aClient.Next);
    end;
  for i := 0 to Brokers.Count - 1 do
    TMQTTClient (Brokers[i]).Parser.KeepAlive := Value;
end;

procedure ServerTimerTask (Data : Pointer);
var
  j : integer;
  bPacket : TMQTTPacket;
  aClient : TMQTTThread;
  aServer : TMQTTServer;
  WillClose : Boolean;
begin
  with PTimerRec (Data)^ do
    begin
      aServer := TMQTTServer (Owner);
      log ('timer ' + No.ToString + ' triggered');
      case No of
        3 : begin
              aClient := TMQTTThread (aServer.Threads.First);
              while aClient <> nil do
                begin
                  if not aClient.Parser.CheckKeepAlive then
                    begin
                      WillClose := true;
                      if Assigned (aServer.FOnFailure) then aServer.FOnFailure (aClient, frKEEPALIVE, WillClose);
                      if WillClose then aClient.Server.Disconnect;
                    end
                  else
                    begin
                      for j := aClient.InFlight.Count - 1 downto 0 do
                        begin
                          bPacket := aClient.InFlight[j];
                          if bPacket.Counter > 0 then
                            begin
                              bPacket.Counter := bPacket.Counter - 1;
                              if bPacket.Counter = 0 then
                                begin
                                  bPacket.Retries := bPacket.Retries + 1;
                                  if bPacket.Retries <= aClient.Parser.MaxRetries then
                                    begin
                                      if bPacket.Publishing then
                                        begin
                                          aClient.InFlight.List.Remove (bPacket);
                                          Log ('Message ' + IntToStr (bPacket.ID) + ' disposed of..');
                                          Log ('Re-issuing Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
                                          SetDup (bPacket.Msg, true);
                                          aClient.DoSend (aClient.Parser, bPacket.ID, bPacket.Retries, bPacket.Msg);
                                          bPacket.Free;
                                        end
                                      else
                                        begin
                                          Log ('Re-issuing PUBREL Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
                                          aClient.Parser.SendPubRel (bPacket.ID, true);
                                          bPacket.Counter := aClient.Parser.RetryTime;
                                        end;
                                    end
                                  else
                                    begin
                                      WillClose := true;
                                      if Assigned (aServer.FOnFailure) then aServer.FOnFailure (aServer, frMAXRETRIES, WillClose);
                                      if WillClose then aClient.Server.Disconnect;
                                    end;
                                end;
                            end;
                        end;
                    end;
                  aClient := TMQTTThread (aClient.Next);
                end;
            end;
        end;
  end;
end;

procedure TMQTTServer.SetTimer (No, Interval: LongWord; Once : boolean);
var
  Flags : Longword;
begin
  if not (No in [1 .. 3]) then exit;
  if Timers[No].Handle <> INVALID_HANDLE_VALUE then TimerDestroy (Timers[No].Handle);
  if Once then Flags := TIMER_FLAG_NONE else Flags := TIMER_FLAG_RESCHEDULE;
  Timers[No].Handle := TimerCreateEx (Interval, TIMER_STATE_ENABLED, Flags, ServerTimerTask, @Timers[No]);
end;

procedure TMQTTServer.KillTimer (No: Longword);
begin
  if not (No in [1 .. 3]) then exit;
  if Timers[No].Handle = INVALID_HANDLE_VALUE then exit;
  TimerDestroy (Timers[No].Handle);
  Timers[No].Handle := INVALID_HANDLE_VALUE;
end;

procedure TMQTTServer.StoreBrokers (anIniFile: string);
var
  i : integer;
  aBroker : TMQTTClient;
  Sections : TStringList;
begin
  Sections := TStringList.Create;
  with TIniFile.Create (anIniFile) do
    begin
      ReadSections (Sections);
      for i := 0 to Sections.Count - 1 do
        if Copy (Sections[i], 1, 6) = 'BROKER' then
          EraseSection (Sections[i]);
      for i := 0 to Brokers.Count - 1 do
        begin
          aBroker := Brokers[i];
          WriteString (format ('BROKER%.3d', [i]), 'Prim Host', aBroker.Host);
          WriteInteger (format ('BROKER%.3d', [i]), 'Port', aBroker.Port);
          WriteBool (format ('BROKER%.3d', [i]), 'Enabled', aBroker.Enabled);
        end;
      Free;
    end;
  Sections.Free;
end;

procedure TMQTTServer.SyncBrokerSubscriptions (aBroker: TMQTTClient);
var
  i, j, k : integer;
  x : cardinal;
  ToSub, ToUnsub : TStringList;
  aClient : TMQTTThread;
  found : boolean;
begin
  ToSub := TStringList.Create;
  ToUnsub := TStringList.Create;
  aClient := TMQTTThread (Threads.First);
  while aClient <> nil do
    begin
      for j := 0 to aClient.Subscriptions.Count - 1 do
        begin
          found := false;
          for k := 0 to ToSub.Count - 1 do
            begin
              if aClient.Subscriptions[j] = ToSub[k] then
                begin
                  found := true;
                  break;
                end;
            end;
          if not found then ToSub.AddObject (aClient.Subscriptions[j], aClient.Subscriptions.Objects[j]);
        end;
      aClient := TMQTTThread (aClient.Next);
    end;
  // add no longer used to unsubscribe
  for i := aBroker.Subscriptions.Count - 1 downto 0 do
    begin
      found := false;
      for j := 0 to ToSub.Count - 1 do
        begin
          if aBroker.Subscriptions[i] = ToSub[j] then
            begin
              x := cardinal (aBroker.Subscriptions.Objects[i]) and $03;      // change to highest
              if x > (cardinal (ToSub.Objects[j]) and $03) then
                ToSub.Objects[j] := TObject (x);
              found := true;
              break;
            end;
        end;
      if not found then
        ToUnsub.AddObject (aBroker.Subscriptions[i], aBroker.Subscriptions.Objects[i]);
    end;
  // remove those already subscribed to
  for i := 0 to aBroker.Subscriptions.Count - 1 do
    begin
      for j := ToSub.Count - 1 downto 0 do
        begin
          if aBroker.Subscriptions[i] = ToSub[j] then
            ToSub.Delete (j);     // already subscribed
        end;
    end;
  for i := 0 to ToSub.Count - 1 do
    aBroker.Subscribe (UTF8String (ToSub[i]), TMQTTQOSType (cardinal (ToSub.Objects[i]) and $03));
  for i := 0 to ToUnsub.Count - 1 do
    aBroker.Unsubscribe (UTF8String (ToUnsub[i]));
  ToSub.Free;
  ToUnsub.Free;
end;

function TMQTTServer.NextMessageID: Word;
var
  j : integer;
  Unused : boolean;
  aMsg : TMQTTPacket;
  aClient : TMQTTThread;
begin
  repeat
    Unused := true;
    MessageID := MessageID + 1;
    if MessageID = 0 then MessageID := 1;   // exclude 0
    aClient := TMQTTThread (Threads.First);
    while aClient <> nil do
      begin
        for j := 0 to aClient.InFlight.Count - 1 do
          begin
            aMsg := aClient.InFlight[j];
            if aMsg.ID = MessageID then
              begin
                Unused := false;
                break;
              end;
          end;
        if not Unused then break
        else aClient := TMQTTThread (aClient.Next);
      end;
  until Unused;
  Result := MessageID;
end;

procedure TMQTTServer.PublishToAll (From : TObject; aTopic : UTF8String; aMessage : AnsiString; aQos : TMQTTQOSType; wasRetained : boolean);
var
  i, j : integer;
  sent : boolean;
  aClient : TMQTTThread;
  aBroker : TMQTTClient;
  bQos : TMQTTQOSType;
begin
  Log ('Publishing -- Was Retained ' + ny[wasRetained]);
  aClient := TMQTTThread (Threads.First);
  while aClient <> nil do
    begin
      if (aClient = From) and (aClient.FBroker) then continue;  // don't send back to self if broker - non standard
      //not LocalBounce then continue;
      sent := false;
      for j := 0 to aClient.Subscriptions.Count - 1 do
        begin
          if IsSubscribed (UTF8String (aClient.Subscriptions[j]), aTopic) then
            begin
              bQos := TMQTTQOSType (cardinal (aClient.Subscriptions.Objects[j]) and $03);
              if aClient.FBroker then
                Log ('Publishing to Broker ' + aClient.Parser.ClientID + ' "' + aTopic + '" Retained ' + ny[wasRetained and aClient.FBroker])
              else
                Log ('Publishing to Client ' + aClient.Parser.ClientID + ' "' + aTopic + '"');
              if bQos > aQos then bQos := aQos;
              aClient.Parser.SendPublish (NextMessageID, aTopic, aMessage, bQos, false, wasRetained and aClient.FBroker);
              sent := true;
              break;    // only do first
            end;
        end;
      if (not sent) and (wasRetained) and (aClient.FBroker) then
        begin
          Log ('Forwarding Retained message to broker');
          aClient.Parser.SendPublish (NextMessageID, aTopic, aMessage, qtAT_LEAST_ONCE, false, true);
        end;
      aClient := TMQTTThread (aClient.Next);
    end;
  for i := 0 to Brokers.Count - 1 do    // brokers get all messages -> downstream
    begin
      aBroker := TMQTTClient (Brokers[i]);
      if aBroker = From then continue;
      if not aBroker.Enabled then continue;
    //  if aBroker then
      Log ('Publishing to Broker ' + string (aBroker.ClientID) + ' "' + string (aTopic) + '" @ ' + QOSNames[aQos] + ' Retained ' + ny[wasretained]);
      aBroker.Publish (aTopic, aMessage, aQos, wasRetained);
   end;
end;

       (*
procedure TMQTTServer.DoClientCreate (Sender: TObject; Client: TWSocketClient);
begin
  with TClient (Client) do
    begin
      Parser.OnPing := RxPing;
      Parser.OnDisconnect := RxDisconnect;
      Parser.OnPublish := RxPublish;
      Parser.OnPubRec := RxPubRec;
      Parser.OnConnect := RxConnect;
      Parser.OnBrokerConnect := RxBrokerConnect;    // non standard
      Parser.OnHeader := RxHeader;
      Parser.MaxRetries := FMaxRetries;
      Parser.RetryTime := FRetryTime;
      OnMon := DoMon;
      OnSubscriptionChange := BkrSubscriptionChange;
  end;
end;         *)
               (*
procedure TMQTTServer.DoClientDisconnect (Sender: TObject;
  Client: TWSocketClient; Error: Word);
var
  aTopic, aMessage : UTF8String;
  aQos : TMQTTQOSType;
begin
  with TClient (Client) do
    begin
      Mon ('Client Disconnected.  Graceful ' + ny[TClient (Client).FGraceful]);
      if (InFlight.Count > 0) or (Releasables.Count > 0) then
        begin
          if Assigned (FOnStoreSession) then
            FOnStoreSession (Client, Parser.ClientID)
          else
            Sessions.StoreSession (Parser.ClientID, TClient (Client));
        end;
      if not FGraceful then
        begin
          aTopic := Parser.WillTopic;
          aMessage := Parser.WillMessage;
          aQos := Parser.WillQos;
          if Assigned (FOnObituary) then
            FOnObituary (Client, aTopic, aMessage, aQos);
          PublishToAll (nil, aTopic, AnsiString (aMessage), aQos);
        end;
    end;
  if Assigned (FOnClientsChange) then
    FOnClientsChange (Server, Server.ClientCount - 1);
end;
              *)

function TMQTTServer.Enabled: boolean;
begin
  Result := FEnable;
end;

function TMQTTServer.GetClient (aClientID: UTF8String): TMQTTThread;
begin
  Result := TMQTTThread (Threads.First);
  while Result <> nil do
    begin
      if Result.Parser.ClientID = aClientID then exit;
      Result := TMQTTThread (Result.Next);
    end;
  Result := nil;
end;

function TMQTTServer.GetClient (aParser: TMQTTParser): TMQTTThread;
begin
  Result := TMQTTThread (Threads.First);
  while Result <> nil do
    begin
      if Result.Parser = aParser then exit;
      Result := TMQTTThread (Result.Next);
    end;
  Result := nil;
end;

procedure TMQTTServer.RxBrokerConnect (Sender: TObject; aProtocol: UTF8String;
  aVersion: byte; aClientID, aUserName, aPassword: UTF8String; aKeepAlive: Word;
  aClean: Boolean);
var
  aClient : TMQTTThread;
begin
  if not (Sender is TMQTTParser) then exit;
  aClient := GetClient (TMQTTParser (Sender));
  if aClient = nil then exit;
  aClient.FBroker := true;
  RxConnect (Sender, aProtocol, aVersion, aClientID, aUserName, aPassword, aKeepAlive, aClean);
end;

procedure TMQTTServer.RxConnect (Sender: TObject; aProtocol: UTF8String;
  aVersion: byte; aClientID, aUserName, aPassword: UTF8String; aKeepAlive: Word;
  aClean: Boolean);
var
  aClient : TMQTTThread;
  Allowed : Boolean;
begin
  Allowed := false;
  if not (Sender is TMQTTParser) then exit;
  aClient := GetClient (TMQTTParser (Sender));
  if aClient = nil then exit;
  aClient.FGraceful := true;
  if Assigned (FOnCheckUser) then FOnCheckUser (Self, aUserName, aPassword, Allowed);
  if Allowed then
    begin
      if aVersion < MinVersion then
        begin
          aClient.Parser.SendConnAck (rcPROTOCOL);  // identifier rejected
          aClient.Server.Disconnect;
        end
      else if (length (aClientID) < 1) or (length (aClientID) > 23) then
        begin
          aClient.Parser.SendConnAck (rcIDENTIFIER);  // identifier rejected
          aClient.Server.Disconnect;
        end
      else if GetClient (aClientID) <> nil then
        begin
          aClient.Parser.SendConnAck (rcIDENTIFIER);  // identifier rejected
          aClient.Server.Disconnect;
        end
      else
        begin
      //    mon ('Client ID ' + ClientID + ' User '  + striUserName + ' Pass ' + PassWord);
          aClient.Parser.Username := aUserName;
          aClient.Parser.Password := aPassword;
          aClient.Parser.ClientID := aClientID;
          aClient.Parser.KeepAlive := aKeepAlive;
          aClient.Parser.Clean := aClean;
          Log ('Clean ' + ny[aClean]);
          if not aClean then
            begin
              if Assigned (FOnRestoreSession) then
                FOnRestoreSession (aClient, aClientID)
              else
                Sessions.RestoreSession (aClientID, aClient);
            end;
          if Assigned (FOnDeleteSession) then
            FOnDeleteSession (aClient, aClientID)
          else
            Sessions.DeleteSession (aClientID);
          aClient.Parser.SendConnAck (rcACCEPTED);
          aClient.FGraceful := false;
          Log ('Accepted. Is Broker ' + ny[aClient.FBroker]);
          if Assigned (FOnClientsChange) then FOnClientsChange (Self, Threads.Count);
        end;
    end
  else
    begin
      aClient.Parser.SendConnAck (rcUSER);
      aClient.Server.Disconnect;
    end;
end;

procedure TMQTTServer.RxDisconnect (Sender: TObject);
var
  aClient : TMQTTThread;
begin
  if not (Sender is TMQTTParser) then exit;
  aClient := GetClient (TMQTTParser (Sender));
  if aClient = nil then exit;
  aClient.FGraceful := true;
end;

procedure TMQTTServer.RxHeader (Sender: TObject; MsgType: TMQTTMessageType;
  Dup: Boolean; Qos: TMQTTQOSType; Retain: Boolean);
begin
  if Assigned (FOnMonHdr) then FOnMonHdr (Self, MsgType, Dup, Qos, Retain);
end;

{ TMQTTClient }

procedure TMQTTClient.Activate (Enable: Boolean);
begin
  if Enable = FEnable then exit;
  FEnable := Enable;
  try
   if FState = stConnected then
      begin
        Parser.SendDisconnect;
        FGraceful := true;
        Link.Disconnect;
      end;
  except
    end;
  if Enable then
    SetTimer (1, 100, true)
  else
    begin
      KillTimer (1);
      KillTimer (2);
      KillTimer (3);
    end;
  if Assigned (FOnEnableChange) then FOnEnableChange (Self);
end;

constructor TMQTTClient.Create;
var
  i : integer;
begin
  inherited Create (false);
  FreeOnTerminate := true;
  Link := TWinsock2TCPClient.Create;
  for i := 1 to 3 do
    begin
      Timers[i].Handle := INVALID_HANDLE_VALUE;
      Timers[i].No := i;
      Timers[i].Owner := Self;
    end;
  FEvent := TEvent.Create (nil, true, false, '');
  FState := stInit;
  FHost := '';
  FUsername := '';
  FPassword := '';
  FPort := 1883;
  FEnable := false;
  FGraceful := false;
  FOnline := false;
  FBroker := false;         // non standard
  FLocalBounce := false;
  FAutoSubscribe := false;
  FMessageID := 0;
  Subscriptions := TStringList.Create;
  Releasables := TMQTTMessageStore.Create;
  Parser := TMQTTParser.Create;
  Parser.OnSend := DoSend;
  Parser.OnConnAck := RxConnAck;
  Parser.OnPublish := RxPublish;
  Parser.OnSubAck := RxSubAck;
  Parser.OnUnsubAck := RxUnsubAck;
  Parser.OnPubAck := RxPubAck;
  Parser.OnPubRel := RxPubRel;
  Parser.OnPubRec := RxPubRec;
  Parser.OnPubComp := RxPubComp;
  Parser.KeepAlive := 10;

  InFlight := TMQTTPacketStore.Create;

end;

destructor TMQTTClient.Destroy;
begin
  FEvent.SetEvent;
  FEvent.Free;
  Releasables.Clear;
  Releasables.Free;
  Subscriptions.Free;
  InFlight.Clear;
  InFlight.Free;
  KillTimer (1);
  KillTimer (2);
  KillTimer (3);
  Link.Free;
  Link := nil;
(*  try
    Link.Close;
  finally
    Link.Free;
  end;       *)
  Parser.Free;
  inherited;
end;

procedure TMQTTClient.RxConnAck (Sender: TObject; aCode: byte);
var
  i : integer;
  x : cardinal;
begin
  Log ('Connection ' + codenames (aCode));
  if aCode = rcACCEPTED then
    begin
      FOnline := true;
      FGraceful := false;
      SetTimer (3, 100, true);  // start retry counters
      if Assigned (FOnOnline) then FOnOnline (Self);
      if (FAutoSubscribe) and (Subscriptions.Count > 0) then
        begin
          for i := 0 to Subscriptions.Count - 1 do
            begin
              x := cardinal (Subscriptions.Objects[i]) and $03;
              Parser.SendSubscribe (NextMessageID, UTF8String (Subscriptions[i]), TMQTTQOSType (x));
            end;
        end;
    end
  else
    Activate (false); // not going to connect
end;
// publishing
procedure TMQTTClient.RxPublish (Sender: TObject; anID: Word; aTopic : UTF8String;
  aMessage : string);
var
  aMsg : TMQTTMessage;
begin
  case Parser.RxQos of
    qtAT_MOST_ONCE  :
      if Assigned (FOnMsg) then FOnMsg (Self, aTopic, aMessage, Parser.RxQos, Parser.RxRetain);
    qtAT_LEAST_ONCE :
      begin
        Parser.SendPubAck (anID);
        if Assigned (FOnMsg) then FOnMsg (Self, aTopic, aMessage, Parser.RxQos, Parser.RxRetain);
      end;
    qtEXACTLY_ONCE  :
      begin
        Parser.SendPubRec (anID);
        aMsg := Releasables.GetMsg (anID);
        if aMsg = nil then
          begin
            Releasables.AddMsg (anID, aTopic, aMessage, Parser.RxQos, 0, 0, Parser.RxRetain);
            Log ('Message ' + IntToStr (anID) + ' stored and idle.');
          end
        else
          Log ('Message ' + IntToStr (anID) + ' already stored.');
      end;
  end;
end;

procedure TMQTTClient.RxPubAck (Sender: TObject; anID: Word);
begin
  InFlight.DelPacket (anID);
  Log ('ACK Message ' + IntToStr (anID) + ' disposed of.');
end;

procedure TMQTTClient.RxPubComp (Sender: TObject; anID: Word);
begin
  InFlight.DelPacket (anID);
  Log ('COMP Message ' + IntToStr (anID) + ' disposed of.');
end;

procedure TMQTTClient.RxPubRec (Sender: TObject; anID: Word);
var
  aPacket : TMQTTPacket;
begin
  aPacket := InFlight.GetPacket (anID);
  if aPacket <> nil then
    begin
      aPacket.Counter := Parser.RetryTime;
      if aPacket.Publishing then
        begin
          aPacket.Publishing := false;
          Log ('REC Message ' + IntToStr (anID) + ' recorded.');
        end
      else
        Log ('REC Message ' + IntToStr (anID) + ' already recorded.');
    end
  else
    Log ('REC Message ' + IntToStr (anID) + ' not found.');
  Parser.SendPubRel (anID);
end;

procedure TMQTTClient.RxPubRel (Sender: TObject; anID: Word);
var
  aMsg : TMQTTMessage;
begin
  aMsg := Releasables.GetMsg (anID);
  if aMsg <> nil then
    begin
      Log ('REL Message ' + IntToStr (anID) + ' publishing @ ' + QOSNames[aMsg.Qos]);
      if Assigned (FOnMsg) then FOnMsg (Self, aMsg.Topic, aMsg.Message, aMsg.Qos, aMsg.Retained);
      Releasables.Remove (aMsg);
      aMsg.Free;
      Log ('REL Message ' + IntToStr (anID) + ' removed from storage.');
    end
  else
    Log ('REL Message ' + IntToStr (anID) + ' has been already removed from storage.');
  Parser.SendPubComp (anID);
end;

procedure TMQTTClient.SetClean (const Value: Boolean);
begin
  Parser.Clean := Value;
end;

procedure TMQTTClient.SetClientID (const Value: UTF8String);
begin
  Parser.ClientID := Value;
end;

procedure TMQTTClient.SetKeepAlive (const Value: Word);
begin
  Parser.KeepAlive := Value;
end;

procedure TMQTTClient.SetMaxRetries (const Value: integer);
begin
  Parser.MaxRetries := Value;
end;

procedure TMQTTClient.SetPassword (const Value: UTF8String);
begin
  Parser.Password := Value;
end;

procedure TMQTTClient.SetRetryTime (const Value: cardinal);
begin
  Parser.RetryTime := Value;
end;

procedure TMQTTClient.SetUsername (const Value: UTF8String);
begin
  Parser.UserName := Value;
end;

procedure TMQTTClient.SetState (NewState: integer);
var
  aClientID : UTF8String;

  function TimeString : UTF8string;
  begin   //  86400  secs
    Result := UTF8String (IntToHex (Trunc (Date), 5) + IntToHex (Trunc (Frac (Time) * 864000), 7));
  end;

begin
  if (FState <> NewState) then
    begin
      Log (StateToStr (FState) + ' to ' + StateToStr (NewState) + '.');
      FState := NewState;
      case NewState of
        stClosed    :
          begin
            KillTimer (2);
            KillTimer (3);
            if Assigned (FOnOffline) and (FOnline) then FOnOffline (Self, FGraceful);
            FOnline := false;
            if FEnable then SetTimer (1, 6000, true);
          end;
        stConnected :
          begin
            FGraceful := false;    // still haven't connected but expect to
            Parser.Reset;
   //   mon ('Time String : ' + Timestring);
   //=   mon ('xaddr ' + Link.GetXAddr);
            aClientID := ClientID;
            if aClientID = '' then
              aClientID := 'CID' + UTF8String (Link.RemotePort.ToString); // + TimeString;
            if Assigned (FOnClientID) then FOnClientID (Self, aClientID);
            ClientID := aClientID;
            if Parser.Clean then
              begin
                InFlight.Clear;
                Releasables.Clear;
              end;
            if FBroker then
              Parser.SendBrokerConnect (aClientID, Parser.UserName, Parser.Password, KeepAlive, Parser.Clean)
            else
              Parser.SendConnect (aClientID, Parser.UserName, Parser.Password, KeepAlive, Parser.Clean);
          end;
      end;  // connected
    end;  // case

  if not (FState in [stClosed]) then FEvent.SetEvent;
end;

procedure ClientTimerTask (Data : Pointer);
var
  i : integer;
  bPacket : TMQTTPacket;
  aClient : TMQTTClient;
  WillClose : Boolean;
begin
  with PTimerRec (Data)^ do
    begin
      aClient := TMQTTClient (Owner);
//      log ('Client timer ' + No.ToString + ' triggered');
      aClient.KillTimer (No);
      case No of
        1 : if aClient.FState in [stClosed, stInit] then aClient.SetState (stConnecting);
        2 : aClient.Ping;
        3 : begin         // send duplicates
              for i := aClient.InFlight.Count - 1 downto 0 do
                begin
                  bPacket := aClient.InFlight.List[i];
                  if bPacket.Counter > 0 then
                    begin
                      bPacket.Counter := bPacket.Counter - 1;
                      if bPacket.Counter = 0 then
                        begin
                          bPacket.Retries := bPacket.Retries + 1;
                          if bPacket.Retries <=  aClient.MaxRetries then
                            begin
                              if bPacket.Publishing then
                                begin
                                  aClient.InFlight.List.Remove (bPacket);
                                  Log ('Message ' + IntToStr (bPacket.ID) + ' disposed of..');
                                  Log ('Re-issuing Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
                                  SetDup (bPacket.Msg, true);
                                  aClient.DoSend (aClient.Parser, bPacket.ID, bPacket.Retries, bPacket.Msg);
                                  bPacket.Free;
                                end
                              else
                                begin
                                  Log ('Re-issuing PUBREL Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
                                  aClient.Parser.SendPubRel (bPacket.ID, true);
                                  bPacket.Counter := aClient.Parser.RetryTime;
                                end;
                            end
                          else
                            begin
                              WillClose := true;
                              if Assigned (aClient.FOnFailure) then aClient.FOnFailure (aClient, frMAXRETRIES, WillClose);
                        //      if WillClose then Link.CloseDelayed;
                            end;
                        end;
                    end;
                end;
              aClient.SetTimer (3, 100, true);
            end;
      end;
  end;
end;

procedure TMQTTClient.SetTimer (No, Interval: LongWord; Once: boolean);
var
  Flags : Longword;
begin
  if not (No in [1 .. 3]) then exit;
  if Timers[No].Handle <> INVALID_HANDLE_VALUE then TimerDestroy (Timers[No].Handle);
  if Once then Flags := TIMER_FLAG_NONE else Flags := TIMER_FLAG_RESCHEDULE;
  Timers[No].Handle := TimerCreateEx (Interval, TIMER_STATE_ENABLED, Flags, ClientTimerTask, @Timers[No]);
end;

procedure TMQTTClient.KillTimer (No: Longword);
begin
  if not (No in [1 .. 3]) then exit;
  if Timers[No].Handle = INVALID_HANDLE_VALUE then exit;
  TimerDestroy (Timers[No].Handle);
  Timers[No].Handle := INVALID_HANDLE_VALUE;
end;

procedure TMQTTClient.SetWill (aTopic, aMessage : UTF8String; aQos: TMQTTQOSType;
  aRetain: Boolean);
begin
  Parser.SetWill (aTopic, aMessage, aQos, aRetain);
end;

procedure TMQTTClient.Subscribe (Topics: TStringList);
var
  j : integer;
  i, x : cardinal;
  anID : Word;
  found : boolean;
begin
  if Topics = nil then exit;
  anID := NextMessageID;
  for i := 0 to Topics.Count - 1 do
    begin
      found := false;
      // 255 denotes acked
      if i > 254 then
        x := (cardinal (Topics.Objects[i]) and $03)
      else
        x := (cardinal (Topics.Objects[i]) and $03) + (anID shl 16) + (i shl 8) ;
      for j := 0 to Subscriptions.Count - 1 do
        if Subscriptions[j] = Topics[i] then
          begin
            found := true;
            Subscriptions.Objects[j] := TObject (x);
            break;
          end;
      if not found then
        Subscriptions.AddObject (Topics[i], TObject (x));
    end;
  Parser.SendSubscribe (anID, Topics);
end;

procedure TMQTTClient.Subscribe (aTopic: UTF8String; aQos: TMQTTQOSType);
var
  i : integer;
  x : cardinal;
  found : boolean;
  anID : Word;
begin
  if aTopic = '' then exit;
  found := false;
  anID := NextMessageID;
  x := (ord (aQos) and $ff) + (anID shl 16);
  for i := 0 to Subscriptions.Count - 1 do
    if Subscriptions[i] = string (aTopic) then
      begin
        found := true;
        Subscriptions.Objects[i] := TObject (x);
        break;
      end;
  if not found then
    Subscriptions.AddObject (string (aTopic), TObject (x));
  Parser.SendSubscribe (anID, aTopic, aQos);
end;

procedure TMQTTClient.Execute;
var
  Buff : array [0..255] of byte;
  closed : boolean;
  count : integer;
  res : boolean;
begin
  while not Terminated do
    begin
      if Link = nil then Terminate;
      case FState of
        stClosed :
          begin
            FEvent.ResetEvent;
            FEvent.WaitFor (INFINITE); // park thread
          end;
        stConnecting :
           begin
            Link.RemotePort := Port;
            Link.RemoteAddress := Host;
            Log ('Connecting to ' + Host + ' on Port ' + IntToStr (Port));
            if Link.Connect then SetState (stConnected) else SetState (stClosed);
          end;
        stConnected :
          begin
            count := 0;
            closed := false;
            res := Link.ReadAvailable (@Buff[0], 256, count, closed);
            if res then Parser.Parse (@Buff[0], count);
            if not res or closed then SetState (stClosed);
          end;
        stQuitting : Terminate;
        end;  // case
    end;
end;

procedure TMQTTClient.DoSend (Sender: TObject; anID : Word; aRetry : integer; aStream: TMemoryStream);
var
  x : byte;
begin
  if (FState = stConnected) and (aStream.Size > 0) then
    begin
      KillTimer (2);       // 75% of keep alive
      if KeepAlive > 0 then SetTimer (2, KeepAlive * 750, true);
      aStream.Seek (0, soFromBeginning);
      x := 0;
      aStream.Read (x, 1);
      if (TMQTTQOSType ((x and $06) shr 1) in [qtAT_LEAST_ONCE, qtEXACTLY_ONCE]) and
         (TMQTTMessageType ((x and $f0) shr 4) in [{mtPUBREL,} mtPUBLISH, mtSUBSCRIBE, mtUNSUBSCRIBE]) and
         (anID > 0) then
        begin
          InFlight.AddPacket (anID, aStream, aRetry, Parser.RetryTime);
          Log ('Message ' + IntToStr (anID) + ' created.');
        end;
      Link.WriteData (aStream.Memory, aStream.Size);
 //     Sleep (0);
    end;
end;

function TMQTTClient.Enabled: boolean;
begin
  Result := FEnable;
end;

function TMQTTClient.GetClean: Boolean;
begin
  Result := Parser.Clean;
end;

function TMQTTClient.GetClientID: UTF8String;
begin
  Result := Parser.ClientID;
end;

function TMQTTClient.GetKeepAlive: Word;
begin
  Result := Parser.KeepAlive;
end;

function TMQTTClient.GetMaxRetries: integer;
begin
  Result := Parser.MaxRetries;
end;

function TMQTTClient.GetPassword: UTF8String;
begin
  Result := Parser.Password;
end;

function TMQTTClient.GetRetryTime: cardinal;
begin
  Result := Parser.RetryTime;
end;

function TMQTTClient.GetUsername: UTF8String;
begin
  Result := Parser.UserName;
end;

procedure TMQTTClient.RxSubAck (Sender: TObject; anID: Word; Qoss : array of TMQTTQosType);
var
  j : integer;
  i, x : cardinal;
begin
  InFlight.DelPacket (anID);
  Log ('Message ' + IntToStr (anID) + ' disposed of.');
  for i := low (Qoss) to high (Qoss) do
    begin
      if i > 254 then break;      // only valid for first 254
      for j := 0 to Subscriptions.Count - 1 do
        begin
          x := cardinal (Subscriptions.Objects[j]);
          if (High (x) = anID) and ((x and $0000ff00) shr 8 = i) then
            Subscriptions.Objects[j] :=  TObject ($ff00 + ord (Qoss[i]));
        end;
    end;
end;

procedure TMQTTClient.RxUnsubAck (Sender: TObject; anID: Word);
begin
  InFlight.DelPacket (anID);
  Log ('Message ' + IntToStr (anID) + ' disposed of.');
end;

function TMQTTClient.NextMessageID: Word;
var
  i : integer;
  Unused : boolean;
  aMsg : TMQTTPacket;
begin
  repeat
    Unused := true;
    FMessageID := FMessageID + 1;
    if FMessageID = 0 then FMessageID := 1;   // exclude 0
    for i := 0 to InFlight.Count - 1 do
      begin
        aMsg := InFlight.List[i];
        if aMsg.ID = FMessageID then
          begin
            Unused := false;
            break;
          end;
      end;
  until Unused;
  Result := FMessageID;
end;

function TMQTTClient.Online: boolean;
begin
  Result := FOnline;
end;

procedure TMQTTClient.Ping;
begin
  Parser.SendPing;
end;

procedure TMQTTClient.Publish (aTopic : UTF8String; aMessage : string; aQos : TMQTTQOSType; aRetain : Boolean);
var
  i : integer;
  found : boolean;
begin
  if FLocalBounce and Assigned (FOnMsg) then
    begin
      found := false;
      for i := 0 to Subscriptions.Count - 1 do
        if IsSubscribed (UTF8String (Subscriptions[i]), aTopic) then
          begin
            found := true;
            break;
          end;
      if found then
        begin
          Parser.RxQos := aQos;
          FOnMsg (Self, aTopic, aMessage, aQos, false);
        end;
    end;
  Parser.SendPublish (NextMessageID, aTopic, aMessage, aQos, false, aRetain);
end;
                (*
procedure TMQTTClient.TimerProc (var aMsg: TMessage);
var
  i : integer;
  bPacket : TMQTTPacket;
  WillClose : Boolean;
begin
  if aMsg.Msg = WM_TIMER then
    begin
      KillTimer (Timers, aMsg.WParam);
      case aMsg.WParam of
        1 : begin
              Mon ('Connecting to ' + Host + ' on Port ' + IntToStr (Port));
              Link.Addr := Host;
              Link.Port := IntToStr (Port);
              Link.Proto := 'tcp';
              try
                Link.Connect;
              except
              end;
            end;
        2 : Ping;
        3 : begin         // send duplicates
              for i := InFlight.Count - 1 downto 0 do
                begin
                  bPacket := InFlight.List[i];
                  if bPacket.Counter > 0 then
                    begin
                      bPacket.Counter := bPacket.Counter - 1;
                      if bPacket.Counter = 0 then
                        begin
                          bPacket.Retries := bPacket.Retries + 1;
                          if bPacket.Retries <=  MaxRetries then
                            begin
                              if bPacket.Publishing then
                                begin
                                  InFlight.List.Remove (bPacket);
                                  mon ('Message ' + IntToStr (bPacket.ID) + ' disposed of..');
                                  mon ('Re-issuing Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
                                  SetDup (bPacket.Msg, true);
                                  DoSend (Parser, bPacket.ID, bPacket.Retries, bPacket.Msg);
                                  bPacket.Free;
                                end
                              else
                                begin
                                  mon ('Re-issuing PUBREL Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
                                  Parser.SendPubRel (bPacket.ID, true);
                                  bPacket.Counter := Parser.RetryTime;
                                end;
                            end
                          else
                            begin
                              WillClose := true;
                              if Assigned (FOnFailure) then FOnFailure (Self, frMAXRETRIES, WillClose);
                              if WillClose then Link.CloseDelayed;
                            end;
                        end;
                    end;
                end;
              SetTimer (Timers, 3, 100, nil);
            end;
      end;
    end;
end;        *)

procedure TMQTTClient.Unsubscribe (Topics: TStringList);
var
  i, J : integer;
begin
  if Topics = nil then exit;
  for i := 0 to Topics.Count - 1 do
    begin
      for j := Subscriptions.Count - 1 downto 0 do
        if Subscriptions[j] = Topics[i] then
          begin
            Subscriptions.Delete (j);
            break;
          end;
    end;
  Parser.SendUnsubscribe (NextMessageID, Topics);
end;

procedure TMQTTClient.Unsubscribe (aTopic: UTF8String);
var
  i : integer;
begin
  if aTopic = '' then exit;
  for i := Subscriptions.Count - 1 downto 0 do
    if Subscriptions[i] = string (aTopic) then
      begin
        Subscriptions.Delete (i);
        break;
      end;
  Parser.SendUnsubscribe (NextMessageID, aTopic);
end;
        (*
procedure TMQTTClient.LinkClosed (Sender: TObject; ErrCode: Word);
begin
//  Mon ('Link Closed...');
  KillTimer (2);
  KillTimer (3);
  if Assigned (FOnOffline) and (FOnline) then FOnOffline (Self, FGraceful);
  FOnline := false;
  if FEnable then SetTimer (1, 6000, true);
end;                   *)

{ TMQTTPacketStore }

function TMQTTPacketStore.AddPacket (anID : Word; aMsg : TMemoryStream; aRetry : cardinal; aCount : cardinal) : TMQTTPacket;
begin
  Result := TMQTTPacket.Create;
  Result.ID := anID;
  Result.Counter := aCount;
  Result.Retries := aRetry;
  aMsg.Seek (0, soFromBeginning);
  Result.Msg.CopyFrom (aMsg, aMsg.Size);
  List.Add (Result);
end;

procedure TMQTTPacketStore.Assign (From : TMQTTPacketStore);
var
  i : integer;
  aPacket, bPacket : TMQTTPacket;
begin
  Clear;
  for i := 0 to From.Count - 1 do
    begin
      aPacket := From[i];
      bPacket := TMQTTPacket.Create;
      bPacket.Assign (aPacket);
      List.Add (bPacket);
    end;
end;

procedure TMQTTPacketStore.Clear;
var
  i : integer;
begin
  for i := 0 to List.Count - 1 do
    TMQTTPacket (List[i]).Free;
  List.Clear;
end;

function TMQTTPacketStore.Count: integer;
begin
  Result := List.Count;
end;

constructor TMQTTPacketStore.Create;
begin
  Stamp := Now;
  List := TList.Create;
end;

procedure TMQTTPacketStore.DelPacket (anID: Word);
var
  i : integer;
  aPacket : TMQTTPacket;
begin
  for i := List.Count - 1 downto 0 do
    begin
      aPacket := List[i];
      if aPacket.ID = anID then
        begin
          List.Remove (aPacket);
          aPacket.Free;
          exit;
        end;
    end;
end;

destructor TMQTTPacketStore.Destroy;
begin
  Clear;
  List.Free;
  inherited;
end;

function TMQTTPacketStore.GetItem (Index: Integer): TMQTTPacket;
begin
  if (Index >= 0) and (Index < Count) then
    Result := List[Index]
  else
    Result := nil;
end;

function TMQTTPacketStore.GetPacket (anID: Word): TMQTTPacket;
var
  i : integer;
begin
  for i := 0 to List.Count - 1 do
    begin
      Result := List[i];
      if Result.ID = anID then exit;
    end;
  Result := nil;
end;

procedure TMQTTPacketStore.Remove (aPacket : TMQTTPacket);
begin
  List.Remove (aPacket);
end;

procedure TMQTTPacketStore.SetItem (Index: Integer; const Value: TMQTTPacket);
begin
  if (Index >= 0) and (Index < Count) then
    List[Index] := Value;
end;

{ TMQTTPacket }

procedure TMQTTPacket.Assign (From: TMQTTPacket);
begin
  ID := From.ID;
  Stamp := From.Stamp;
  Counter := From.Counter;
  Retries := From.Retries;
  Msg.Clear;
  From.Msg.Seek (0, soFromBeginning);
  Msg.CopyFrom (From.Msg, From.Msg.Size);
  Publishing := From.Publishing;
end;

constructor TMQTTPacket.Create;
begin
  ID := 0;
  Stamp := Now;
  Publishing := true;
  Counter := 0;
  Retries := 0;
  Msg := TMemoryStream.Create;
end;

destructor TMQTTPacket.Destroy;
begin
  Msg.Free;
  inherited;
end;

{ TMQTTMessage }

procedure TMQTTMessage.Assign (From: TMQTTMessage);
begin
  ID := From.ID;
  Stamp := From.Stamp;
  LastUsed := From.LastUsed;
  Retained := From.Retained;
  Counter := From.Counter;
  Retries := From.Retries;
  Topic := From.Topic;
  Message := From.Message;
  Qos := From.Qos;
end;

constructor TMQTTMessage.Create;
begin
  ID := 0;
  Stamp := Now;
  LastUsed := Stamp;
  Retained := false;
  Counter := 0;
  Retries := 0;
  Qos := qtAT_MOST_ONCE;
  Topic := '';
  Message := '';
end;

destructor TMQTTMessage.Destroy;
begin
  inherited;
end;

{ TMQTTMessageStore }

function TMQTTMessageStore.AddMsg (anID: Word; aTopic : UTF8String; aMessage : AnsiString; aQos : TMQTTQOSType;
  aRetry, aCount: cardinal; aRetained : Boolean) : TMQTTMessage;
begin
  Result := TMQTTMessage.Create;
  Result.ID := anID;
  Result.Topic := aTopic;
  Result.Message := aMessage;
  Result.Qos := aQos;
  Result.Counter := aCount;
  Result.Retries := aRetry;
  Result.Retained := aRetained;
  List.Add (Result);
end;

procedure TMQTTMessageStore.Assign (From: TMQTTMessageStore);
var
  i : integer;
  aMsg, bMsg : TMQTTMessage;
begin
  Clear;
  for i := 0 to From.Count - 1 do
    begin
      aMsg := From[i];
      bMsg := TMQTTMessage.Create;
      bMsg.Assign (aMsg);
      List.Add (bMsg);
    end;
end;

procedure TMQTTMessageStore.Clear;
var
  i : integer;
begin
  for i := 0 to List.Count - 1 do
    TMQTTMessage (List[i]).Free;
  List.Clear;
end;

function TMQTTMessageStore.Count: integer;
begin
  Result := List.Count;
end;

constructor TMQTTMessageStore.Create;
begin
  Stamp := Now;
  List := TList.Create;
end;

procedure TMQTTMessageStore.DelMsg (anID: Word);
var
  i : integer;
  aMsg : TMQTTMessage;
begin
  for i := List.Count - 1 downto 0 do
    begin
      aMsg := List[i];
      if aMsg.ID = anID then
        begin
          List.Remove (aMsg);
          aMsg.Free;
          exit;
        end;
    end;
end;

destructor TMQTTMessageStore.Destroy;
begin
  Clear;
  List.Free;
  inherited;
end;

function TMQTTMessageStore.GetItem (Index: Integer): TMQTTMessage;
begin
  if (Index >= 0) and (Index < Count) then
    Result := List[Index]
  else
    Result := nil;
end;

function TMQTTMessageStore.GetMsg (anID: Word): TMQTTMessage;
var
  i : integer;
begin
  for i := 0 to List.Count - 1 do
    begin
      Result := List[i];
      if Result.ID = anID then exit;
    end;
  Result := nil;
end;

procedure TMQTTMessageStore.Remove (aMsg: TMQTTMessage);
begin
  List.Remove (aMsg);
end;

procedure TMQTTMessageStore.SetItem (Index: Integer; const Value: TMQTTMessage);
begin
  if (Index >= 0) and (Index < Count) then
    List[Index] := Value;
end;

{ TMQTTSession }

constructor TMQTTSession.Create;
begin
  ClientID := '';
  Stamp := Now;
  InFlight := TMQTTPacketStore.Create;
  Releasables := TMQTTMessageStore.Create;
end;

destructor TMQTTSession.Destroy;
begin
  InFlight.Clear;
  InFlight.Free;
  Releasables.Clear;
  Releasables.Free;
  inherited;
end;

{ TMQTTSessionStore }

procedure TMQTTSessionStore.Clear;
var
  i : integer;
begin
  for i := 0 to List.Count - 1 do
    TMQTTSession (List[i]).Free;
  List.Clear;
end;

function TMQTTSessionStore.Count: integer;
begin
  Result := List.Count;
end;

constructor TMQTTSessionStore.Create;
begin
  Stamp := Now;
  List := TList.Create;
end;

procedure TMQTTSessionStore.DeleteSession (ClientID: UTF8String);
var
  aSession : TMQTTSession;
begin
  aSession := GetSession (ClientID);
  if aSession <> nil then
    begin
      List.Remove (aSession);
      aSession.Free;
    end;
end;

destructor TMQTTSessionStore.Destroy;
begin
  Clear;
  List.Free;
  inherited;
end;

function TMQTTSessionStore.GetItem (Index: Integer): TMQTTSession;
begin
  if (Index >= 0) and (Index < Count) then
    Result := List[Index]
  else
    Result := nil;
end;

function TMQTTSessionStore.GetSession (ClientID: UTF8String): TMQTTSession;
var
  i : integer;
begin
  for i := 0 to List.Count - 1 do
    begin
      Result := List[i];
      if Result.ClientID = ClientID then exit;
    end;
  Result := nil;
end;

procedure TMQTTSessionStore.RestoreSession (ClientID: UTF8String;
  aClient: TMQTTThread);
var
  aSession : TMQTTSession;
begin
  aClient.InFlight.Clear;
  aClient.Releasables.Clear;
  aSession := GetSession (ClientID);
  if aSession <> nil then
    begin
      aClient.InFlight.Assign (aSession.InFlight);
      aClient.Releasables.Assign (aSession.Releasables);
    end;
end;

procedure TMQTTSessionStore.RestoreSession (ClientID: UTF8String;
  aClient: TMQTTClient);
var
  aSession : TMQTTSession;
begin
  aClient.InFlight.Clear;
  aClient.Releasables.Clear;
  aSession := GetSession (ClientID);
  if aSession <> nil then
    begin
      aClient.InFlight.Assign (aSession.InFlight);
      aClient.Releasables.Assign (aSession.Releasables);
    end;
end;

procedure TMQTTSessionStore.StoreSession (ClientID: UTF8String;
  aClient: TMQTTClient);
var
  aSession : TMQTTSession;
begin
  aSession := GetSession (ClientID);
  if aSession <> nil then
    begin
      aSession := TMQTTSession.Create;
      aSession.ClientID := ClientID;
      List.Add (aSession);
    end;

  aSession.InFlight.Assign (aClient.InFlight);
  aSession.Releasables.Assign (aClient.Releasables);
end;

procedure TMQTTSessionStore.StoreSession (ClientID: UTF8String;
  aClient: TMQTTThread);
var
  aSession : TMQTTSession;
begin
  aClient.InFlight.Clear;
  aClient.Releasables.Clear;
  aSession := GetSession (ClientID);
  if aSession <> nil then
    begin
      aSession := TMQTTSession.Create;
      aSession.ClientID := ClientID;
      List.Add (aSession);
    end;
  aSession.InFlight.Assign (aClient.InFlight);
  aSession.Releasables.Assign (aClient.Releasables);
end;

procedure TMQTTSessionStore.SetItem (Index: Integer; const Value: TMQTTSession);
begin
  if (Index >= 0) and (Index < Count) then
    List[Index] := Value;
end;

end.

