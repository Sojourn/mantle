------------------------------- MODULE Mantle -------------------------------
EXTENDS Integers, Naturals, Sequences, FiniteSets, TLC

CONSTANTS CONTROLLER, REGIONS, MAX_SEQUENCE

NODES == {CONTROLLER} \cup REGIONS

MESSAGE_TYPES == {
    "SYNC", \* Sent by regions to the controller
    "GIFT"  \* Sent by regions to other regions
}

(*--algorithm MantleAlg {
variables
    network = [node \in NODES |-> <<>>];
    sequences = [node \in NODES |-> 0];

macro SendMessage(node, message) {
    assert node \in NODES;
    assert message.type \in MESSAGE_TYPES;

    network[node] := Append(network[node], message);
}

macro RecvMessage(node, message) {
    assert node \in NODES;

    await network[node] # <<>>;
    message := Head(network[node]);
    network[node] := Tail(network[node]);
}

macro RecordInc(ledger, sequence) {
    ledger[(sequence + 0) % 4].incs := ledger[(sequence + 0) % 4].incs + 1;
}

macro RecordDec(ledger, sequence) {
    ledger[(sequence + 2) % 4].decs := ledger[(sequence + 2) % 4].decs + 1;
}

process(controller = CONTROLLER)
    variables
        message = [type |-> "START", data |-> <<>>];
        refs = Cardinality(REGIONS);
        write_barriers = <<>>;
        write_barrier_index = 0;
{
controller_start:
    while (sequences[CONTROLLER] # MAX_SEQUENCE) {
        write_barriers := <<>>;

controller_receive_syncs:
        while (Len(write_barriers) < Cardinality(REGIONS)) {
            RecvMessage(CONTROLLER, message);
            assert(message.type = "SYNC");
            write_barriers := Append(write_barriers, message.data);
        };

controller_apply_incs_init:
        write_barrier_index := 1;

controller_apply_incs_loop:
        while (write_barrier_index <= Len(write_barriers)) {
            refs := refs + write_barriers[write_barrier_index].incs;
            write_barrier_index := write_barrier_index + 1;
        };

controller_apply_decs_init:
        write_barrier_index := 1;

controller_apply_decs_loop:
        while (write_barrier_index <= Len(write_barriers)) {
            refs := refs - write_barriers[write_barrier_index].decs;
            write_barrier_index := write_barrier_index + 1;
        };

        \* This is the primary invariant we care about.
        assert refs >= 0;

        sequences[CONTROLLER] := sequences[CONTROLLER] + 1;
    };
}

process(region \in REGIONS)
    variables
        message = [type |-> "START", data |-> <<>>];
        ledger = [i \in 0..3 |-> [incs |-> 0, decs |-> 0]];
        refs = 1;
        tokens = 2;
{
region_start:
    while (sequences[self] # MAX_SEQUENCE) {
region_consume_tokens:
        while (tokens # 0) {
            either {
                skip;
            }
            or {
                await refs > 0;
    
                RecordInc(ledger, sequences[self]);
                refs := refs + 1;
            }
            or {
                await refs > 0;
    
                RecordDec(ledger, sequences[self]);
                refs := refs - 1;
            }
            or {
                await refs > 0;
                
                RecordInc(ledger, sequences[self]);
    
                with (region \in REGIONS) {
                    SendMessage(region, [
                        type |-> "GIFT",
                        data |-> <<1>>
                    ]);
                };
            }
            or {
                RecvMessage(self, message);
                assert message.type = "GIFT";
                refs := refs + message.data[1];
            };

            tokens := tokens - 1;
        };

        \* Wait for the controller to finish processing sync messages from the previous sequence.
        await sequences[self] = sequences[CONTROLLER];

        SendMessage(CONTROLLER, [
            type |-> "SYNC",
            data |-> ledger[sequences[self] % 4]
        ]);

        ledger[sequences[self] % 4] := [incs |-> 0, decs |-> 0];
        sequences[self] := sequences[self] + 1;
        tokens := 2;
    };
}

}
--*)
\* BEGIN TRANSLATION (chksum(pcal) = "f9e01aae" /\ chksum(tla) = "c3daf840")
\* Process variable message of process controller at line 43 col 9 changed to message_
\* Process variable refs of process controller at line 44 col 9 changed to refs_
VARIABLES network, sequences, pc, message_, refs_, write_barriers, 
          write_barrier_index, message, ledger, refs, tokens

vars == << network, sequences, pc, message_, refs_, write_barriers, 
           write_barrier_index, message, ledger, refs, tokens >>

ProcSet == {CONTROLLER} \cup (REGIONS)

Init == (* Global variables *)
        /\ network = [node \in NODES |-> <<>>]
        /\ sequences = [node \in NODES |-> 0]
        (* Process controller *)
        /\ message_ = [type |-> "START", data |-> <<>>]
        /\ refs_ = Cardinality(REGIONS)
        /\ write_barriers = <<>>
        /\ write_barrier_index = 0
        (* Process region *)
        /\ message = [self \in REGIONS |-> [type |-> "START", data |-> <<>>]]
        /\ ledger = [self \in REGIONS |-> [i \in 0..3 |-> [incs |-> 0, decs |-> 0]]]
        /\ refs = [self \in REGIONS |-> 1]
        /\ tokens = [self \in REGIONS |-> 2]
        /\ pc = [self \in ProcSet |-> CASE self = CONTROLLER -> "controller_start"
                                        [] self \in REGIONS -> "region_start"]

controller_start == /\ pc[CONTROLLER] = "controller_start"
                    /\ IF sequences[CONTROLLER] # MAX_SEQUENCE
                          THEN /\ write_barriers' = <<>>
                               /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_receive_syncs"]
                          ELSE /\ pc' = [pc EXCEPT ![CONTROLLER] = "Done"]
                               /\ UNCHANGED write_barriers
                    /\ UNCHANGED << network, sequences, message_, refs_, 
                                    write_barrier_index, message, ledger, refs, 
                                    tokens >>

controller_receive_syncs == /\ pc[CONTROLLER] = "controller_receive_syncs"
                            /\ IF Len(write_barriers) < Cardinality(REGIONS)
                                  THEN /\ Assert(CONTROLLER \in NODES, 
                                                 "Failure of assertion at line 26, column 5 of macro called at line 54, column 13.")
                                       /\ network[CONTROLLER] # <<>>
                                       /\ message_' = Head(network[CONTROLLER])
                                       /\ network' = [network EXCEPT ![CONTROLLER] = Tail(network[CONTROLLER])]
                                       /\ Assert((message_'.type = "SYNC"), 
                                                 "Failure of assertion at line 55, column 13.")
                                       /\ write_barriers' = Append(write_barriers, message_'.data)
                                       /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_receive_syncs"]
                                  ELSE /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_apply_incs_init"]
                                       /\ UNCHANGED << network, message_, 
                                                       write_barriers >>
                            /\ UNCHANGED << sequences, refs_, 
                                            write_barrier_index, message, 
                                            ledger, refs, tokens >>

controller_apply_incs_init == /\ pc[CONTROLLER] = "controller_apply_incs_init"
                              /\ write_barrier_index' = 1
                              /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_apply_incs_loop"]
                              /\ UNCHANGED << network, sequences, message_, 
                                              refs_, write_barriers, message, 
                                              ledger, refs, tokens >>

controller_apply_incs_loop == /\ pc[CONTROLLER] = "controller_apply_incs_loop"
                              /\ IF write_barrier_index <= Len(write_barriers)
                                    THEN /\ refs_' = refs_ + write_barriers[write_barrier_index].incs
                                         /\ write_barrier_index' = write_barrier_index + 1
                                         /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_apply_incs_loop"]
                                    ELSE /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_apply_decs_init"]
                                         /\ UNCHANGED << refs_, 
                                                         write_barrier_index >>
                              /\ UNCHANGED << network, sequences, message_, 
                                              write_barriers, message, ledger, 
                                              refs, tokens >>

controller_apply_decs_init == /\ pc[CONTROLLER] = "controller_apply_decs_init"
                              /\ write_barrier_index' = 1
                              /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_apply_decs_loop"]
                              /\ UNCHANGED << network, sequences, message_, 
                                              refs_, write_barriers, message, 
                                              ledger, refs, tokens >>

controller_apply_decs_loop == /\ pc[CONTROLLER] = "controller_apply_decs_loop"
                              /\ IF write_barrier_index <= Len(write_barriers)
                                    THEN /\ refs_' = refs_ - write_barriers[write_barrier_index].decs
                                         /\ write_barrier_index' = write_barrier_index + 1
                                         /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_apply_decs_loop"]
                                         /\ UNCHANGED sequences
                                    ELSE /\ Assert(refs_ >= 0, 
                                                   "Failure of assertion at line 78, column 9.")
                                         /\ sequences' = [sequences EXCEPT ![CONTROLLER] = sequences[CONTROLLER] + 1]
                                         /\ pc' = [pc EXCEPT ![CONTROLLER] = "controller_start"]
                                         /\ UNCHANGED << refs_, 
                                                         write_barrier_index >>
                              /\ UNCHANGED << network, message_, 
                                              write_barriers, message, ledger, 
                                              refs, tokens >>

controller == controller_start \/ controller_receive_syncs
                 \/ controller_apply_incs_init
                 \/ controller_apply_incs_loop
                 \/ controller_apply_decs_init
                 \/ controller_apply_decs_loop

region_start(self) == /\ pc[self] = "region_start"
                      /\ IF sequences[self] # MAX_SEQUENCE
                            THEN /\ pc' = [pc EXCEPT ![self] = "region_consume_tokens"]
                            ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                      /\ UNCHANGED << network, sequences, message_, refs_, 
                                      write_barriers, write_barrier_index, 
                                      message, ledger, refs, tokens >>

region_consume_tokens(self) == /\ pc[self] = "region_consume_tokens"
                               /\ IF tokens[self] # 0
                                     THEN /\ \/ /\ TRUE
                                                /\ UNCHANGED <<network, message, ledger, refs>>
                                             \/ /\ refs[self] > 0
                                                /\ ledger' = [ledger EXCEPT ![self][((sequences[self]) + 0) % 4].incs = ledger[self][((sequences[self]) + 0) % 4].incs + 1]
                                                /\ refs' = [refs EXCEPT ![self] = refs[self] + 1]
                                                /\ UNCHANGED <<network, message>>
                                             \/ /\ refs[self] > 0
                                                /\ ledger' = [ledger EXCEPT ![self][((sequences[self]) + 2) % 4].decs = ledger[self][((sequences[self]) + 2) % 4].decs + 1]
                                                /\ refs' = [refs EXCEPT ![self] = refs[self] - 1]
                                                /\ UNCHANGED <<network, message>>
                                             \/ /\ refs[self] > 0
                                                /\ ledger' = [ledger EXCEPT ![self][((sequences[self]) + 0) % 4].incs = ledger[self][((sequences[self]) + 0) % 4].incs + 1]
                                                /\ \E region \in REGIONS:
                                                     /\ Assert(region \in NODES, 
                                                               "Failure of assertion at line 19, column 5 of macro called at line 116, column 21.")
                                                     /\ Assert((                    [
                                                                   type |-> "GIFT",
                                                                   data |-> <<1>>
                                                               ]).type \in MESSAGE_TYPES, 
                                                               "Failure of assertion at line 20, column 5 of macro called at line 116, column 21.")
                                                     /\ network' = [network EXCEPT ![region] = Append(network[region], (                    [
                                                                                                   type |-> "GIFT",
                                                                                                   data |-> <<1>>
                                                                                               ]))]
                                                /\ UNCHANGED <<message, refs>>
                                             \/ /\ Assert(self \in NODES, 
                                                          "Failure of assertion at line 26, column 5 of macro called at line 123, column 17.")
                                                /\ network[self] # <<>>
                                                /\ message' = [message EXCEPT ![self] = Head(network[self])]
                                                /\ network' = [network EXCEPT ![self] = Tail(network[self])]
                                                /\ Assert(message'[self].type = "GIFT", 
                                                          "Failure of assertion at line 124, column 17.")
                                                /\ refs' = [refs EXCEPT ![self] = refs[self] + message'[self].data[1]]
                                                /\ UNCHANGED ledger
                                          /\ tokens' = [tokens EXCEPT ![self] = tokens[self] - 1]
                                          /\ pc' = [pc EXCEPT ![self] = "region_consume_tokens"]
                                          /\ UNCHANGED sequences
                                     ELSE /\ sequences[self] = sequences[CONTROLLER]
                                          /\ Assert(CONTROLLER \in NODES, 
                                                    "Failure of assertion at line 19, column 5 of macro called at line 134, column 9.")
                                          /\ Assert((                        [
                                                        type |-> "SYNC",
                                                        data |-> ledger[self][sequences[self] % 4]
                                                    ]).type \in MESSAGE_TYPES, 
                                                    "Failure of assertion at line 20, column 5 of macro called at line 134, column 9.")
                                          /\ network' = [network EXCEPT ![CONTROLLER] = Append(network[CONTROLLER], (                        [
                                                                                            type |-> "SYNC",
                                                                                            data |-> ledger[self][sequences[self] % 4]
                                                                                        ]))]
                                          /\ ledger' = [ledger EXCEPT ![self][sequences[self] % 4] = [incs |-> 0, decs |-> 0]]
                                          /\ sequences' = [sequences EXCEPT ![self] = sequences[self] + 1]
                                          /\ tokens' = [tokens EXCEPT ![self] = 2]
                                          /\ pc' = [pc EXCEPT ![self] = "region_start"]
                                          /\ UNCHANGED << message, refs >>
                               /\ UNCHANGED << message_, refs_, write_barriers, 
                                               write_barrier_index >>

region(self) == region_start(self) \/ region_consume_tokens(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == controller
           \/ (\E self \in REGIONS: region(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
