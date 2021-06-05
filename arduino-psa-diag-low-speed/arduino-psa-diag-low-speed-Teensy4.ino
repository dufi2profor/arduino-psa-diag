
/*
Copyright 2020-2021, Ludwig V. <https://github.com/ludwig-v>
Date: 2021-03-06

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License at <http://www.gnu.org/licenses/> for
more details.

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
*/

/*
Date: 2021-06-03
Ported to Teensy 4.0 by DusanKo / profor / proforsk
Aim is to use it on slow speed CAN bus, Teensy must be
connected to TJA1055/3 CAN transciever
Sketch uses Teensys powerful FlexCAN_T4 library.

Date: 2021-06-05
Changed bools to one uint32_t flag register
Removed Thread library replaced by native Teensys elapsedMillis library

*/

/////////////////////
//    Libraries    //
/////////////////////

#include <FlexCAN_T4.h>
#include <elapsedMillis.h>


/////////////////////
//  Configuration  //
/////////////////////

#define DEBUG_LEDS

#define SKETCH_VERSION                    "1.6"
#define CAN_RCV_BUFFER                    40
#define CAN_DEFAULT_DELAY                 5         // Delay between multiframes
#define MAX_DATA_LENGTH                   512
#define SERIAL_SPEED                      115200
#define CAN_SPEED                         125000    // Entertainment CAN bus - Low Speed

////////////////////
// Initialization //
////////////////////

////////////////////
//   Variables    //
////////////////////


// My variables

uint16_t CAN_EMIT_ID = 0x764; // NAC
uint16_t CAN_RECV_ID = 0x664; // NAC

char  tmp[4];
char  UnlockCMD[5];
char  SketchVersion[4];
char  receiveDiagFrameData[MAX_DATA_LENGTH];
uint16_t  additionalFrameID = 0;
uint8_t DiagSess = 0x03;
uint16_t  receiveDiagFrameRead = 0;
uint16_t  receiveDiagFrameSize = 0;
uint8_t receiveDiagDataPos = 0;
uint16_t  receiveDiagFrameAlreadyFlushed = 0;
uint8_t LIN = 0;
uint32_t  lastCMDSent = 0;
uint16_t  UnlockKey = 0;
uint8_t UnlockService = 0;
uint8_t framesDelayInput = CAN_DEFAULT_DELAY;
uint8_t framesDelay = CAN_DEFAULT_DELAY;
uint16_t  sendingAdditionalDiagFramesPos = 0;
uint32_t  lastSendingAdditionalDiagFrames = 0;
uint32_t  lastKeepAliveReceived = 0;
uint8_t sendKeepAliveType = 'U';

#define FLAG_PARSING_CAN                  0         //bool parsingCAN = false;
#define FLAG_READING_CAN                  1         //bool readingCAN = false;
#define FLAG_LOCK                         2         //bool Lock = false;
#define FLAG_SEND_KEEPALIVES              3         //bool sendKeepAlives = false;
#define FLAG_SENDING_ADD_DIAG_FRAMES      4         //bool sendingAdditionalDiagFrames = false;
#define FLAG_WAITINIG_UNLOCK              5         //bool waitingUnlock = false;
#define FLAG_WAIT_REPLY_SERIAL_CMD        6         //bool waitingReplySerialCMD = false;
#define FLAG_MULTI_FRAME_OVERFLOW         7         //bool multiframeOverflow = false;

#define FLAG_DUMP                         31        //bool Dump = false; // Passive dump mode, dump Diagbox frames

uint32_t  flags = 0;

//  added pin definitions for TJA1055 (in case of low speed CAN) and debug outs 
#define TJA_MASTER_ERR                    2         //  pins for control of TJA1055/3
#define TJA_MASTER_STB                    11        //  https://www.nxp.com/docs/en/data-sheet/TJA1055.pdf
#define TJA_MASTER_EN                     10
#ifdef  DEBUG_LEDS
  #define LED_DEBUG1                        16
  #define LED_DEBUG2                        17
  #define LED_DEBUG3                        18
#endif

#define INTERVAL_KEEPALIVES               1000
#define INTERVAL_PARSE_CAN                4
#define INTERVAL_ADDITIONAL_DIAG          1

elapsedMillis keepalives;
elapsedMillis parsecan;
elapsedMillis additionalDiag;

CAN_message_t canMsgRcvBuffer[CAN_RCV_BUFFER];
FlexCAN_T4<CAN2, RX_SIZE_256, TX_SIZE_256> can_master;  //  pin 0 RX, pin 1 TX, replace CAN2 with other available on Teensy4 if needed

/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/

void setup() {
  
  Serial.begin(SERIAL_SPEED);

#ifdef  DEBUG_LEDS
  pinMode(LED_DEBUG1,  OUTPUT);
  pinMode(LED_DEBUG2,  OUTPUT);
  pinMode(LED_DEBUG3,  OUTPUT);
#endif
  
  can_master.begin();
  can_master.setBaudRate(CAN_SPEED);
  
  pinMode(TJA_MASTER_EN,  OUTPUT);
  pinMode(TJA_MASTER_STB,  OUTPUT);
  digitalWriteFast(TJA_MASTER_EN,  HIGH);
  digitalWriteFast(TJA_MASTER_STB,  HIGH);  
  
  strcpy(SketchVersion, SKETCH_VERSION);

  for (uint16_t i = 0; i < CAN_RCV_BUFFER; i++) {
    canMsgRcvBuffer[i].id = 0;
    canMsgRcvBuffer[i].len = 0;
  }

  keepalives = 0;
  parsecan = 0;
  additionalDiag = 0;
  
}

/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/


void loop() {
  
  if (Serial.available() > 0) {
    
    recvWithTimeout();

  } else {

    readCAN();

    if  (parsecan > INTERVAL_PARSE_CAN) {
      parsecan = 0;
      parseCAN();
    }
    
    if  (keepalives > INTERVAL_KEEPALIVES)  {
      keepalives = 0;
      sendKeepAlive();
    }
    
    if  (additionalDiag > INTERVAL_ADDITIONAL_DIAG) {
      additionalDiag = 0;
      sendAdditionalDiagFrames();
    }

  }

}

/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/

void readCAN() {
  
  if (!bitRead(flags, FLAG_LOCK) && !bitRead(flags, FLAG_READING_CAN)) {
    CAN_message_t canMsgRcv;
    bitSet(flags, FLAG_READING_CAN);
//    readingCAN = true;
    if (can_master.read(canMsgRcv)) {
      for (uint16_t i = 0; i < CAN_RCV_BUFFER; i++) {
        if (canMsgRcvBuffer[i].id == 0) { // Free in buffer
          canMsgRcvBuffer[i] = canMsgRcv;
          parsecan = INTERVAL_PARSE_CAN;    //  will execute parseCAN right after this function ends
//          parseCANThread.runned(); // Delay thread execution to avoid loosing messages
          break;
        }
      }      
    }
//    readingCAN = false;
    bitClear(flags, FLAG_READING_CAN);
  }
  
}


/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/

void parseCAN() {

#ifdef  DEBUG_LEDS
  digitalWriteFast(LED_DEBUG1,  !digitalReadFast(LED_DEBUG1));
#endif

  if (!bitRead(flags, FLAG_LOCK) && !bitRead(flags, FLAG_PARSING_CAN) && !bitRead(flags, FLAG_SENDING_ADD_DIAG_FRAMES)) {
    
    bitSet(flags, FLAG_PARSING_CAN);
//    parsingCAN = true;
    for (uint16_t t = 0; t < CAN_RCV_BUFFER; t++) {
      if (canMsgRcvBuffer[t].id > 0) {
        
        uint16_t id = canMsgRcvBuffer[t].id;
        bool encap = false;
        
        if (canMsgRcvBuffer[t].buf[0] >= 0x40) { // UDS or KWP with LIN ECUs, remove encapsulation
            for (uint16_t i = 1; i < canMsgRcvBuffer[t].len; i++) {
                canMsgRcvBuffer[t].buf[i - 1] = canMsgRcvBuffer[t].buf[i];
            }
            canMsgRcvBuffer[t].len--;
            encap = true;
        }

        uint16_t len = canMsgRcvBuffer[t].len;

        if (canMsgRcvBuffer[t].buf[0] < 0x10 && canMsgRcvBuffer[t].buf[1] == 0x7E) {
          lastKeepAliveReceived = millis();
        } else if (canMsgRcvBuffer[t].buf[0] < 0x10 && canMsgRcvBuffer[t].buf[1] == 0x3E) {
          bitClear(flags, FLAG_SEND_KEEPALIVES);
//          sendKeepAlives = false; // Diagbox or external tool sending keep-alives, stop sending ours
        } else if (bitRead(flags, FLAG_DUMP)) {
          if (bitRead(flags, FLAG_WAIT_REPLY_SERIAL_CMD) && len == 3 && canMsgRcvBuffer[t].buf[0] == 0x30 && canMsgRcvBuffer[t].buf[1] == 0x00) { // Acknowledgement Write
            framesDelay = canMsgRcvBuffer[t].buf[2];

            if (LIN > 0)
              sendingAdditionalDiagFramesPos = 10; // 5 bytes already sent
            else
              sendingAdditionalDiagFramesPos = 12; // 6 bytes already sent
            bitSet(flags, FLAG_SENDING_ADD_DIAG_FRAMES);
//            sendingAdditionalDiagFrames = true;
            bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//            waitingReplySerialCMD = false;
            lastCMDSent = 0;
          } else if (len > 2 && canMsgRcvBuffer[t].buf[0] >= 0x10 && canMsgRcvBuffer[t].buf[0] <= 0x15) { // Acknowledgement Read
            receiveDiagFrameSize = ((canMsgRcvBuffer[t].buf[0] - 0x10) * 256) + canMsgRcvBuffer[t].buf[1];

            if (bitRead(flags, FLAG_WAIT_REPLY_SERIAL_CMD) && LIN) {
              CAN_message_t diagFrame;
              diagFrame.buf[0] = LIN;
              diagFrame.buf[1] = 0x30;
              diagFrame.buf[2] = 0x00;
              diagFrame.buf[3] = framesDelayInput;
              diagFrame.id = CAN_EMIT_ID;
              diagFrame.len = 4;
              can_master.write(diagFrame);
              bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//              waitingReplySerialCMD = false;
              lastCMDSent = 0;
            } else if (bitRead(flags, FLAG_WAIT_REPLY_SERIAL_CMD)) {
              CAN_message_t diagFrame;
              diagFrame.buf[0] = 0x30;
              diagFrame.buf[1] = 0x00;
              diagFrame.buf[2] = framesDelayInput;
              diagFrame.id = CAN_EMIT_ID;
              diagFrame.len = 3;
              can_master.write(diagFrame);
              bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//              waitingReplySerialCMD = false;
              lastCMDSent = 0;
            }
            receiveDiagMultiFrame(canMsgRcvBuffer[t]);
          } else if (len > 1 && canMsgRcvBuffer[t].buf[0] >= 0x20 && canMsgRcvBuffer[t].buf[0] <= 0x2F) {
            if (!bitRead(flags, FLAG_MULTI_FRAME_OVERFLOW))
              receiveAdditionalDiagFrame(canMsgRcvBuffer[t], encap);
          } else if (len == 3 && canMsgRcvBuffer[t].buf[0] == 0x30 && canMsgRcvBuffer[t].buf[1] == 0x00) {
            // Ignore in dump mode
            framesDelay = canMsgRcvBuffer[t].buf[2];
          } else {
            snprintf(tmp, 4, "%02X", id);
            Serial.print(tmp);
            Serial.print(":");
            for (uint16_t i = 1; i < len; i++) { // Strip first byte = Data length
              snprintf(tmp, 3, "%02X", canMsgRcvBuffer[t].buf[i]);
              Serial.print(tmp);
            }
            Serial.println();
          }
        } else {
          if (id == CAN_RECV_ID) {
            if (bitRead(flags, FLAG_WAIT_REPLY_SERIAL_CMD) && len == 3 && canMsgRcvBuffer[t].buf[0] == 0x30 && canMsgRcvBuffer[t].buf[1] == 0x00) { // Acknowledgement Write
              framesDelay = canMsgRcvBuffer[t].buf[2];

              if (LIN > 0)
                sendingAdditionalDiagFramesPos = 10; // 5 bytes already sent
              else
                sendingAdditionalDiagFramesPos = 12; // 6 bytes already sent
              bitSet(flags, FLAG_SENDING_ADD_DIAG_FRAMES);
//              sendingAdditionalDiagFrames = true;
              bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//              waitingReplySerialCMD = false;
              lastCMDSent = 0;
            } else if (len > 2 && canMsgRcvBuffer[t].buf[0] >= 0x10 && canMsgRcvBuffer[t].buf[0] <= 0x15) { // Acknowledgement Read
              receiveDiagFrameSize = ((canMsgRcvBuffer[t].buf[0] - 0x10) * 256) + canMsgRcvBuffer[t].buf[1];

              if (bitRead(flags, FLAG_WAIT_REPLY_SERIAL_CMD) && LIN) {
                CAN_message_t diagFrame;
                diagFrame.buf[0] = LIN;
                diagFrame.buf[1] = 0x30;
                diagFrame.buf[2] = 0x00;
                diagFrame.buf[3] = framesDelayInput;
                diagFrame.id = CAN_EMIT_ID;
                diagFrame.len = 4;
                can_master.write(diagFrame);
                bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//                waitingReplySerialCMD = false;
                lastCMDSent = 0;
              } else if (bitRead(flags, FLAG_WAIT_REPLY_SERIAL_CMD)) {
                CAN_message_t diagFrame;
                diagFrame.buf[0] = 0x30;
                diagFrame.buf[1] = 0x00;
                diagFrame.buf[2] = framesDelayInput;
                diagFrame.id = CAN_EMIT_ID;
                diagFrame.len = 3;
                can_master.write(diagFrame);
                bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//                waitingReplySerialCMD = false;
                lastCMDSent = 0;
              }
              receiveDiagMultiFrame(canMsgRcvBuffer[t]);
            } else if (len > 1 && canMsgRcvBuffer[t].buf[0] >= 0x20 && canMsgRcvBuffer[t].buf[0] <= 0x2F) {
              if (!bitRead(flags, FLAG_MULTI_FRAME_OVERFLOW))
                receiveAdditionalDiagFrame(canMsgRcvBuffer[t], encap);
            } else {
              for (uint16_t i = 1; i < len; i++) { // Strip first byte = Data length
                snprintf(tmp, 3, "%02X", canMsgRcvBuffer[t].buf[i]);
                Serial.print(tmp);
              }
              Serial.println();
            }
          }
        }

        if (canMsgRcvBuffer[t].buf[0] < 0x10 && (canMsgRcvBuffer[t].buf[1] == 0x7F || (lastCMDSent > 0 && millis() - lastCMDSent >= 1000))) { // Error / No answer
          if (canMsgRcvBuffer[t].buf[2] == 0x3E) {
            bitClear(flags, FLAG_SEND_KEEPALIVES);
//            sendKeepAlives = false; // Stop sending Keep-Alives
          }
          bitClear(flags, FLAG_WAITINIG_UNLOCK);
//          waitingUnlock = false;
          bitClear(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//          waitingReplySerialCMD = false;
          lastCMDSent = 0;
        } else if (bitRead(flags, FLAG_WAITINIG_UNLOCK) && canMsgRcvBuffer[t].buf[0] < 0x10 && canMsgRcvBuffer[t].buf[1] == 0x50 && canMsgRcvBuffer[t].buf[2] == DiagSess) {
          sendDiagFrame(UnlockCMD, strlen(UnlockCMD));
        }

        if (bitRead(flags, FLAG_SEND_KEEPALIVES) && lastKeepAliveReceived > 0 && millis() - lastKeepAliveReceived >= 1000) { // ECU connection lost, no answer
          Serial.println("7F3E03"); // Custom error
          bitClear(flags, FLAG_SEND_KEEPALIVES);
//          sendKeepAlives = false; // Stop sending Keep-Alives
        }

        if (bitRead(flags, FLAG_WAITINIG_UNLOCK) && canMsgRcvBuffer[t].buf[0] < 0x10 && canMsgRcvBuffer[t].buf[1] == 0x67 && canMsgRcvBuffer[t].buf[2] == UnlockService) {
          char SeedKey[9];
          char UnlockCMD_Seed[16];

          snprintf(tmp, 3, "%02X", canMsgRcvBuffer[t].buf[3]);
          strcpy(SeedKey, tmp);
          snprintf(tmp, 3, "%02X", canMsgRcvBuffer[t].buf[4]);
          strcat(SeedKey, tmp);
          snprintf(tmp, 3, "%02X", canMsgRcvBuffer[t].buf[5]);
          strcat(SeedKey, tmp);
          snprintf(tmp, 3, "%02X", canMsgRcvBuffer[t].buf[6]);
          strcat(SeedKey, tmp);
          uint32_t Key = compute_response(UnlockKey, strtoul(SeedKey, NULL, 16));
          snprintf(SeedKey, 9, "%08lX", Key);

          strcpy(UnlockCMD_Seed, "27");
          snprintf(tmp, 3, "%02X", (UnlockService + 1)); // Answer
          strcat(UnlockCMD_Seed, tmp);
          strcat(UnlockCMD_Seed, SeedKey);

          if (bitRead(flags, FLAG_DUMP)) {
            snprintf(tmp, 4, "%02X", CAN_EMIT_ID);
            Serial.print(tmp);
            Serial.print(":");
            Serial.println(UnlockCMD_Seed);
          }

          sendDiagFrame(UnlockCMD_Seed, strlen(UnlockCMD_Seed));
          bitClear(flags, FLAG_WAITINIG_UNLOCK);
//          waitingUnlock = false;
        }

        canMsgRcvBuffer[t].id = 0;
      }
    }
//    parsingCAN = false;
    bitClear(flags, FLAG_PARSING_CAN);
  }
}

/************************************************************************************************************************/

void receiveAdditionalDiagFrame(CAN_message_t &frame, bool encap) {
  uint16_t i = 0;
  uint16_t frameOrder = 0;
  uint16_t framePos = 0;
  uint8_t maxBytesperFrame = 8;

  if (encap) // LIN
    maxBytesperFrame--;

  if (frame.buf[0] == 0x20) {
    if (receiveDiagDataPos == 0) {
      receiveDiagDataPos += 15; // 21 > 2F
    } else {
      receiveDiagDataPos += 16; // 20 > 2F
    }
  }
  if (receiveDiagFrameAlreadyFlushed > 0) {
    frameOrder = frame.buf[0] - 0x20;
  } else {
    frameOrder = frame.buf[0] - 0x21 + receiveDiagDataPos;
  }

  for (i = 1; i < frame.len; i++) {
    snprintf(tmp, 3, "%02X", frame.buf[i]);

    if (receiveDiagFrameAlreadyFlushed > 0) {
      framePos = (frameOrder * (maxBytesperFrame - 1) * 2) + ((i - 1) * 2) + 1;
    } else {
      // 6 bytes already received + 7 bytes max per frame
      framePos = ((maxBytesperFrame - 2) * 2) + (frameOrder * (maxBytesperFrame - 1) * 2) + ((i - 1) * 2) + 1;
    }

    if (framePos > MAX_DATA_LENGTH) { // Avoid overflow
      bitSet(flags, FLAG_MULTI_FRAME_OVERFLOW);
//      multiframeOverflow = true;
      if (bitRead(flags, FLAG_DUMP)) {
        i = (framePos - 1) / 2;
        Serial.print("Warning: Truncated data ");
        Serial.print(i);
        Serial.print("/");
        Serial.println(receiveDiagFrameSize);
      }
      break;
    }

    receiveDiagFrameData[framePos - 1] = tmp[0];
    receiveDiagFrameData[framePos] = tmp[1];

    receiveDiagFrameRead += 2;
  }

  if (framesDelay > 0) { // Can't flush buffer fast enough if no delay (some frames will be lost, data will be truncated)
    if (frame.buf[0] == 0x2F) {
      if (bitRead(flags, FLAG_DUMP) && receiveDiagFrameAlreadyFlushed == 0) {
        snprintf(tmp, 4, "%02X", frame.id);
        Serial.print(tmp);
        Serial.print(":");
      }

      receiveDiagFrameAlreadyFlushed += receiveDiagFrameRead;

      receiveDiagFrameData[framePos + 1] = '\0';
      receiveDiagDataPos = receiveDiagFrameRead = 0;

      Serial.print(receiveDiagFrameData);
    }
  }

  if ((receiveDiagFrameRead + receiveDiagFrameAlreadyFlushed) == (receiveDiagFrameSize * 2) || framePos > MAX_DATA_LENGTH) { // Data complete or overflow
    if (bitRead(flags, FLAG_DUMP) && receiveDiagFrameAlreadyFlushed == 0) {
      snprintf(tmp, 4, "%02X", frame.id);
      Serial.print(tmp);
      Serial.print(":");
    }

    receiveDiagFrameData[receiveDiagFrameRead] = '\0';
    receiveDiagDataPos = receiveDiagFrameRead = receiveDiagFrameAlreadyFlushed = 0;

    Serial.println(receiveDiagFrameData);

    framesDelay = CAN_DEFAULT_DELAY; // Restore default delay
  }
}

/************************************************************************************************************************/

void receiveDiagMultiFrame(CAN_message_t &frame) {
  uint16_t i = 0;

  receiveDiagFrameAlreadyFlushed = 0;
  bitClear(flags, FLAG_MULTI_FRAME_OVERFLOW);
//  multiframeOverflow = false;
  receiveDiagFrameRead = 0;

  for (i = 2; i < frame.len; i++) {
    snprintf(tmp, 3, "%02X", frame.buf[i]);
    receiveDiagFrameData[receiveDiagFrameRead] = tmp[0];
    receiveDiagFrameData[receiveDiagFrameRead + 1] = tmp[1];

    receiveDiagFrameRead += 2;
  }
}


/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/

void sendKeepAlive() {

  CAN_message_t diagFrame;
  if (bitRead(flags, FLAG_SEND_KEEPALIVES)) {
    if (sendKeepAliveType == 'K') { // KWP
      if (LIN > 0) {
        diagFrame.buf[0] = LIN;
        diagFrame.buf[1] = 0x01;
        diagFrame.buf[2] = 0x3E;

        diagFrame.id = CAN_EMIT_ID;
        diagFrame.len = 3;
      } else {
        diagFrame.buf[0] = 0x01;
        diagFrame.buf[1] = 0x3E;

        diagFrame.id = CAN_EMIT_ID;
        diagFrame.len = 2;
      }
    } else { // UDS
      if (LIN > 0) {
        diagFrame.buf[0] = LIN;
        diagFrame.buf[1] = 0x02;
        diagFrame.buf[2] = 0x3E;
        diagFrame.buf[3] = 0x00;

        diagFrame.id = CAN_EMIT_ID;
        diagFrame.len = 4;
      } else {
        diagFrame.buf[0] = 0x02;
        diagFrame.buf[1] = 0x3E;
        diagFrame.buf[2] = 0x00;

        diagFrame.id = CAN_EMIT_ID;
        diagFrame.len = 3;
      }
    }

    can_master.write(diagFrame);
  }

  return;
}

/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/

void sendAdditionalDiagFrames() {
  if (!bitRead(flags, FLAG_LOCK) && bitRead(flags, FLAG_SENDING_ADD_DIAG_FRAMES) && millis() - lastSendingAdditionalDiagFrames >= framesDelay) {
    lastSendingAdditionalDiagFrames = millis();

    uint16_t i = 0;
    uint16_t frameLen = 0;
    uint8_t tmpFrame[8] = {0,0,0,0,0,0,0,0};
    CAN_message_t diagFrame;

    for (i = sendingAdditionalDiagFramesPos; i < receiveDiagFrameRead; i += 2) {
      sendingAdditionalDiagFramesPos = i;
      tmpFrame[frameLen] = ahex2int(receiveDiagFrameData[i], receiveDiagFrameData[(i + 1)]);
      frameLen++;

      if (LIN > 0) {
        if (frameLen > 0 && (frameLen % 7) == 0) { // Multi-frames
          diagFrame.buf[0] = LIN;
          diagFrame.buf[1] = additionalFrameID;
          diagFrame.buf[2] = tmpFrame[0];
          diagFrame.buf[3] = tmpFrame[1];
          diagFrame.buf[4] = tmpFrame[2];
          diagFrame.buf[5] = tmpFrame[3];
          diagFrame.buf[6] = tmpFrame[4];
          diagFrame.buf[7] = tmpFrame[5];

          frameLen = 8;
          i -= 4; // First bytes are used by diag data

          additionalFrameID++;
          if (additionalFrameID > 0x2F) {
            additionalFrameID = 0x20;
          }

          diagFrame.id = CAN_EMIT_ID;
          diagFrame.len = frameLen;
          can_master.write(diagFrame);

          frameLen = 0;

          return;
          
        } else if ((i + 2) == receiveDiagFrameRead) {
          diagFrame.buf[0] = LIN;
          diagFrame.buf[1] = additionalFrameID;
          diagFrame.buf[2] = tmpFrame[0];
          diagFrame.buf[3] = tmpFrame[1];
          diagFrame.buf[4] = tmpFrame[2];
          diagFrame.buf[5] = tmpFrame[3];
          diagFrame.buf[6] = tmpFrame[4];
          diagFrame.buf[7] = tmpFrame[5];

          frameLen = frameLen + 2;
          additionalFrameID = 0x00;

          diagFrame.id = CAN_EMIT_ID;
          diagFrame.len = frameLen;
          can_master.write(diagFrame);

          receiveDiagDataPos = receiveDiagFrameRead = 0;

          sendingAdditionalDiagFramesPos = 0;
          lastSendingAdditionalDiagFrames = 0;
          bitClear(flags, FLAG_SENDING_ADD_DIAG_FRAMES);
//          sendingAdditionalDiagFrames = false;
        }

      } else {
        
        if (frameLen > 0 && (frameLen % 8) == 0) { // Multi-frames
          diagFrame.buf[0] = additionalFrameID;
          diagFrame.buf[1] = tmpFrame[0];
          diagFrame.buf[2] = tmpFrame[1];
          diagFrame.buf[3] = tmpFrame[2];
          diagFrame.buf[4] = tmpFrame[3];
          diagFrame.buf[5] = tmpFrame[4];
          diagFrame.buf[6] = tmpFrame[5];
          diagFrame.buf[7] = tmpFrame[6];

          frameLen = 8;

          i -= 2; // First byte is used by diag data

          additionalFrameID++;
          if (additionalFrameID > 0x2F) {
            additionalFrameID = 0x20;
          }

          diagFrame.id = CAN_EMIT_ID;
          diagFrame.len = frameLen;
          can_master.write(diagFrame);

          frameLen = 0;

          return;
          
        } else if ((i + 2) == receiveDiagFrameRead) {
          diagFrame.buf[0] = additionalFrameID;
          diagFrame.buf[1] = tmpFrame[0];
          diagFrame.buf[2] = tmpFrame[1];
          diagFrame.buf[3] = tmpFrame[2];
          diagFrame.buf[4] = tmpFrame[3];
          diagFrame.buf[5] = tmpFrame[4];
          diagFrame.buf[6] = tmpFrame[5];
          diagFrame.buf[7] = tmpFrame[6];

          frameLen = frameLen + 1;
          additionalFrameID = 0x00;

          diagFrame.id = CAN_EMIT_ID;
          diagFrame.len = frameLen;
          can_master.write(diagFrame);

          receiveDiagDataPos = receiveDiagFrameRead = 0;

          sendingAdditionalDiagFramesPos = 0;
          lastSendingAdditionalDiagFrames = 0;
          bitClear(flags, FLAG_SENDING_ADD_DIAG_FRAMES);
//          sendingAdditionalDiagFrames = false;
        }
      }
    }
  }
  return;
}

/************************************************************************************************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/


/* https://github.com/ludwig-v/psa-seedkey-algorithm */
long transform(uint8_t data_msb, uint8_t data_lsb, uint8_t sec[]) {
  long data = (data_msb << 8) | data_lsb;
  long result = ((data % sec[0]) * sec[2]) - ((data / sec[0]) * sec[1]);
  if (result < 0)
    result += (sec[0] * sec[2]) + sec[1];
  return result;
}

/************************************************************************************************************************/

uint32_t compute_response(uint16_t pin, uint32_t chg) {
  uint8_t sec_1[3] = {0xB2,0x3F,0xAA};
  uint8_t sec_2[3] = {0xB1,0x02,0xAB};

  long res_msb = transform((pin >> 8), (pin & 0xFF), sec_1) | transform(((chg >> 24) & 0xFF), (chg & 0xFF), sec_2);
  long res_lsb = transform(((chg >> 16) & 0xFF), ((chg >> 8) & 0xFF), sec_1) | transform((res_msb >> 8), (res_msb & 0xFF), sec_2);
  return (res_msb << 16) | res_lsb;
}

/************************************************************************************************************************/

uint16_t int_pow(uint16_t base, uint16_t exp) {
  uint16_t result = 1;
  while (exp) {
    if (exp % 2)
      result *= base;
    exp /= 2;
    base *= base;
  }
  return result;
}

/************************************************************************************************************************/

uint16_t ahex2int(char a, char b) {
  a = (a <= '9') ? a - '0' : (a & 0x7) + 9;
  b = (b <= '9') ? b - '0' : (b & 0x7) + 9;

  return (a << 4) + b;
}

/************************************************************************************************************************/

void sendDiagFrame(char * data, uint16_t frameFullLen) {
  uint16_t i = 0;
  uint16_t frameLen = 0;
  uint8_t tmpFrame[8] = {0,0,0,0,0,0,0,0};
  CAN_message_t diagFrame;

  for (i = 0; i < frameFullLen && i < 16; i += 2) {
    if (isxdigit(data[i]) && isxdigit(data[i + 1])) {
      tmpFrame[frameLen] = ahex2int(data[i], data[(i + 1)]);
      frameLen++;
    } else {
      if (data[i] == -16) {
        Serial.println("000000");
      } else {
        Serial.println("7F0000");
      }
      return;
    }
  }

  if (LIN > 0) {
    if (frameLen > 6) { // Multi-frames
      diagFrame.buf[0] = LIN;
      diagFrame.buf[1] = 0x10;
      diagFrame.buf[2] = (frameFullLen / 2);
      diagFrame.buf[3] = tmpFrame[0];
      diagFrame.buf[4] = tmpFrame[1];
      diagFrame.buf[5] = tmpFrame[2];
      diagFrame.buf[6] = tmpFrame[3];
      diagFrame.buf[7] = tmpFrame[4];

      frameLen = 8;
      additionalFrameID = 0x21;
    } else {

      diagFrame.buf[0] = LIN;
      diagFrame.buf[1] = frameLen;
      diagFrame.buf[2] = tmpFrame[0];
      diagFrame.buf[3] = tmpFrame[1];
      diagFrame.buf[4] = tmpFrame[2];
      diagFrame.buf[5] = tmpFrame[3];
      diagFrame.buf[6] = tmpFrame[4];
      diagFrame.buf[7] = tmpFrame[5];

      frameLen = frameLen + 2;
      additionalFrameID = 0x00;

      receiveDiagDataPos = receiveDiagFrameRead = 0;
    }
  } else {
    if (frameLen > 7) { // Multi-frames
      diagFrame.buf[0] = 0x10;
      diagFrame.buf[1] = (frameFullLen / 2);
      diagFrame.buf[2] = tmpFrame[0];
      diagFrame.buf[3] = tmpFrame[1];
      diagFrame.buf[4] = tmpFrame[2];
      diagFrame.buf[5] = tmpFrame[3];
      diagFrame.buf[6] = tmpFrame[4];
      diagFrame.buf[7] = tmpFrame[5];

      frameLen = 8;
      additionalFrameID = 0x21;
    } else {

      diagFrame.buf[0] = frameLen;
      diagFrame.buf[1] = tmpFrame[0];
      diagFrame.buf[2] = tmpFrame[1];
      diagFrame.buf[3] = tmpFrame[2];
      diagFrame.buf[4] = tmpFrame[3];
      diagFrame.buf[5] = tmpFrame[4];
      diagFrame.buf[6] = tmpFrame[5];
      diagFrame.buf[7] = tmpFrame[6];

      frameLen = frameLen + 1;
      additionalFrameID = 0x00;

      receiveDiagDataPos = receiveDiagFrameRead = 0;
    }
  }

  diagFrame.id = CAN_EMIT_ID;
  diagFrame.len = frameLen;

  can_master.write(diagFrame);

  return;
}

/************************************************************************************************************************/

uint16_t pos = 0;
void recvWithTimeout() {
  uint32_t lastCharMillis = 0;
  char rc;

  lastCharMillis = millis();
  while (Serial.available() > 0) {
    rc = Serial.read();

    receiveDiagFrameData[pos] = rc;
    receiveDiagFrameRead = pos;

    if (millis() - lastCharMillis >= 1000 || rc == '\n' || pos >= MAX_DATA_LENGTH) {
      receiveDiagFrameData[pos] = '\0';

      if (receiveDiagFrameData[0] == '>') { // IDs Pair changing
        pos = 0;
        char * ids = strtok(receiveDiagFrameData + 1, ":");
        while (ids != NULL) {
          if (pos == 0) {
            CAN_EMIT_ID = strtoul(ids, NULL, 16);
          } else if (pos == 1) {
            CAN_RECV_ID = strtoul(ids, NULL, 16);
          }
          pos++;
          ids = strtok(NULL, ":");
        }
        LIN = 0;
        bitClear(flags, FLAG_DUMP);
//        Dump = false;
        bitClear(flags, FLAG_SEND_KEEPALIVES);
//        sendKeepAlives = false;
        Serial.print(CAN_EMIT_ID, HEX);
        Serial.print(F(" / "));
        Serial.print(CAN_RECV_ID, HEX);
        Serial.println("   OK");
      } else if (receiveDiagFrameData[0] == 'T') { // Change CAN multiframes delay
        framesDelayInput = strtoul(receiveDiagFrameData + 1, NULL, 10);
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == ':') { // Unlock with key
        pos = 0;
        char * ids = strtok(receiveDiagFrameData + 1, ":");
        while (ids != NULL) {
          if (pos == 0) {
            UnlockKey = strtoul(ids, NULL, 16);
          } else if (pos == 1) {
            UnlockService = strtoul(ids, NULL, 16);
          } else if (pos == 2) {
            DiagSess = strtoul(ids, NULL, 16);
          }
          pos++;
          ids = strtok(NULL, ":");
        }
        snprintf(tmp, 3, "%02X", UnlockService);

        strcpy(UnlockCMD, "27");
        strcat(UnlockCMD, tmp);

        char diagCMD[5] = "10";
        snprintf(tmp, 3, "%02X", DiagSess);
        strcat(diagCMD, tmp);
        sendDiagFrame(diagCMD, strlen(diagCMD));

        if (DiagSess == 0xC0) { // KWP
          sendKeepAliveType = 'K';
        } else { // UDS
          sendKeepAliveType = 'U';
        }
        bitSet(flags, FLAG_SEND_KEEPALIVES);
//        sendKeepAlives = true;

        bitSet(flags, FLAG_WAITINIG_UNLOCK);
//        waitingUnlock = true;
      } else if (receiveDiagFrameData[0] == 'V') {
        Serial.println(SketchVersion);
      } else if (receiveDiagFrameData[0] == 'L') {
        LIN = strtoul(receiveDiagFrameData + 1, NULL, 16);
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == 'U') {
        LIN = 0;
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == 'N') {
        bitClear(flags, FLAG_DUMP);
//        Dump = false;
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == 'X') {
        bitClear(flags, FLAG_DUMP);        
//        Dump = true;
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == 'K') {
        bitSet(flags, FLAG_SEND_KEEPALIVES);
//        sendKeepAlives = true;
        if (receiveDiagFrameData[1] == 'U' || receiveDiagFrameData[1] == 'K') {
          sendKeepAliveType = receiveDiagFrameData[1];
        }
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == 'S') {
        bitClear(flags, FLAG_SEND_KEEPALIVES);
//        sendKeepAlives = false;
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == 'R') {
        can_master.begin();
        can_master.setBaudRate(125000);
        Serial.println("OK");
      } else if (receiveDiagFrameData[0] == '?') {
        snprintf(tmp, 4, "%02X", CAN_EMIT_ID);
        Serial.print(tmp);
        Serial.print(":");
        snprintf(tmp, 4, "%02X", CAN_RECV_ID);
        Serial.println(tmp);
      } else if ((receiveDiagFrameRead - 1) % 2) {
        char tmpFrame[16];
        for (uint16_t i = 0; i < receiveDiagFrameRead && i < 16; i++) {
          tmpFrame[i] = receiveDiagFrameData[i];
        }
        sendDiagFrame(tmpFrame, receiveDiagFrameRead);

        bitSet(flags, FLAG_WAIT_REPLY_SERIAL_CMD);
//        waitingReplySerialCMD = true;
        lastCMDSent = millis();
      } else {
        Serial.println("7F0000");
      }
      pos = 0;
    } else {
      pos++;
    }
  }

  return;
}
